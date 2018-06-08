package com.iravid.fs2.kafka.client

import cats.effect._, cats.implicits._
import cats.effect.concurrent.{ Deferred, Ref }
import com.iravid.fs2.kafka.EnvT
import com.iravid.fs2.kafka.codecs.KafkaDecoder
import com.iravid.fs2.kafka.model.{ ByteRecord, ConsumerMessage, Result }
import fs2._
import org.apache.kafka.common.TopicPartition

case class PartitionedRecordStream[F[_], T](
  commitQueue: CommitQueue[F],
  records: Stream[F, (TopicPartition, Stream[F, ConsumerMessage[Result, T]])])

object PartitionedRecordStream {
  case class PartitionHandle[F[_]](data: async.mutable.Queue[F, ByteRecord],
                                   interruption: Deferred[F, Either[Throwable, Unit]]) {
    def records(implicit F: Concurrent[F]): Stream[F, ByteRecord] =
      data.dequeue.interruptWhen(interruption.get)
  }

  object PartitionHandle {
    def fromTopicPartition[F[_]: Concurrent](
      tp: TopicPartition,
      bufferSize: Int): F[(TopicPartition, PartitionHandle[F])] =
      for {
        queue   <- async.boundedQueue[F, ByteRecord](bufferSize)
        promise <- Deferred[F, Either[Throwable, Unit]]
      } yield (tp, PartitionHandle(queue, promise))
  }

  def resources[F[_]: ConcurrentEffect, T: KafkaDecoder](
    settings: ConsumerSettings
  )(implicit timer: Timer[F], consumer: Consumer[F]) =
    for {
      commitQueue      <- CommitQueue.create[F](settings.maxPendingCommits)
      partitionTracker <- Ref[F].of(Map.empty[TopicPartition, PartitionHandle[F]])
      partitionsOut <- async
                        .unboundedQueue[F, (TopicPartition, Stream[F, ConsumerMessage[Result, T]])]

      rebalanceQueue <- async.synchronousQueue[F, Rebalance]
      rebalanceListener: Rebalance.Listener[F] = rebalanceQueue.enqueue1(_)

      commits    = commitQueue.queue.dequeue
      polls      = Stream(Poll) ++ Stream.repeatEval(timer.sleep(settings.pollInterval).as(Poll))
      rebalances = rebalanceQueue.dequeue

      pollingLoopShutdown <- Deferred[F, Either[Throwable, Unit]]

      _ <- Concurrent[F].start {
            commits
              .either(polls)
              .either(rebalances)
              .interruptWhen(pollingLoopShutdown.get)
              .evalMap {
                case Left(Left(req)) =>
                  (consumer.commit(req.asOffsetMap).void.attempt >>= req.promise.complete).void
                case Left(Right(Poll)) =>
                  for {
                    records <- consumer.poll(settings.pollTimeout, settings.wakeupTimeout)
                    tracker <- partitionTracker.get
                    _ <- records.traverse_ { record =>
                          tracker
                            .get(new TopicPartition(record.topic, record.partition))
                            .traverse_(_.data.enqueue1(record))
                        }
                  } yield ()

                case Right(Rebalance.Assign(partitions)) =>
                  for {
                    tracker <- partitionTracker.get
                    handles <- partitions.traverse(
                                PartitionHandle
                                  .fromTopicPartition(_, settings.partitionOutputBufferSize))
                    _ <- partitionTracker.set(tracker ++ handles)
                    _ <- handles.traverse_ {
                          case (tp, h) =>
                            partitionsOut.enqueue1((tp, h.records through deserialize[F, T]))
                        }
                  } yield ()

                case Right(Rebalance.Revoke(partitions)) =>
                  for {
                    tracker <- partitionTracker.get
                    handles = partitions.flatMap(tracker.get)
                    _ <- handles.traverse_(_.interruption.complete(Right(())))
                    _ <- partitionTracker.set(tracker -- partitions)
                  } yield ()
              }
              .compile
              .drain
          }
    } yield (commitQueue, partitionsOut, pollingLoopShutdown.complete(Right(())), rebalanceListener)

  def apply[F[_]: ConcurrentEffect, T: KafkaDecoder](settings: ConsumerSettings,
                                                     subscription: Subscription)(
    implicit
    timer: Timer[F],
    consumer: Consumer[F]): Resource[F, PartitionedRecordStream[F, T]] =
    Resource
      .make(resources(settings)) {
        case (_, _, shutdown, _) =>
          for {
            _ <- consumer.unsubscribe
            _ <- shutdown
          } yield ()
      }
      .flatMap {
        case (commitQueue, partitionsOut, _, rebalanceListener) =>
          Resource.liftF {
            consumer
              .subscribe(subscription, rebalanceListener)
              .as(PartitionedRecordStream(commitQueue, partitionsOut.dequeue))
          }
      }

  def deserialize[F[_], T: KafkaDecoder]: Pipe[F, ByteRecord, ConsumerMessage[Result, T]] =
    _.map(rec => EnvT(rec, KafkaDecoder[T].decode(rec)))
}
