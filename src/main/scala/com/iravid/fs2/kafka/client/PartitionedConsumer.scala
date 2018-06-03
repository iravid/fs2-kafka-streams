package com.iravid.fs2.kafka.client

import cats.effect._, cats.implicits._
import cats.effect.concurrent.{ Deferred, Ref }
import com.iravid.fs2.kafka.client.codecs.KafkaDecoder
import fs2._
import java.util.{ Collection => JCollection, Properties }
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.concurrent.duration._

case class PartitionedConsumer[F[_], T](
  commitQueue: CommitQueue[F],
  records: Stream[F, (TopicPartition, Stream[F, ConsumerMessage[Result, T]])])

object PartitionedConsumer {
  import KafkaConsumerFunctions._

  sealed trait Rebalance
  object Rebalance {
    case class Assign(partitions: List[TopicPartition]) extends Rebalance
    case class Revoke(partitions: List[TopicPartition]) extends Rebalance
  }

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
    settings: Properties,
    maxPendingCommits: Int,
    partitionBufferSize: Int,
    pollTimeout: FiniteDuration,
    pollInterval: FiniteDuration
  )(implicit timer: Timer[F]) =
    for {
      consumer         <- createConsumer[F](settings)
      commitQueue      <- CommitQueue.create[F](maxPendingCommits)
      partitionTracker <- Ref[F].of(Map.empty[TopicPartition, PartitionHandle[F]])
      partitionsOut <- async
                        .unboundedQueue[F, (TopicPartition, Stream[F, ConsumerMessage[Result, T]])]

      rebalanceQueue <- async.synchronousQueue[F, Rebalance]
      rebalanceListener = new ConsumerRebalanceListener {
        def onPartitionsAssigned(jpartitions: JCollection[TopicPartition]): Unit =
          Effect[F]
            .runAsync(rebalanceQueue.enqueue1(Rebalance.Assign(jpartitions.asScala.toList)))(_ =>
              IO.unit)
            .unsafeRunSync()

        def onPartitionsRevoked(jpartitions: JCollection[TopicPartition]): Unit =
          Effect[F]
            .runAsync(rebalanceQueue.enqueue1(Rebalance.Revoke(jpartitions.asScala.toList)))(_ =>
              IO.unit)
            .unsafeRunSync()
      }

      commits    = commitQueue.queue.dequeue
      polls      = Stream(Poll) ++ Stream.repeatEval(timer.sleep(pollInterval).as(Poll))
      rebalances = rebalanceQueue.dequeue

      pollingLoopShutdown <- Deferred[F, Either[Throwable, Unit]]

      _ <- Concurrent[F].start {
            commits
              .either(polls)
              .either(rebalances)
              .interruptWhen(pollingLoopShutdown.get)
              .evalMap {
                case Left(Left(req)) =>
                  (commit(consumer, req.asOffsetMap).void.attempt >>= req.promise.complete).void
                case Left(Right(Poll)) =>
                  for {
                    records <- poll(consumer, pollTimeout)
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
                                PartitionHandle.fromTopicPartition(_, partitionBufferSize))
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
    } yield
      (
        commitQueue,
        partitionsOut,
        pollingLoopShutdown.complete(Right(())),
        consumer,
        rebalanceListener)

  def apply[F[_]: ConcurrentEffect, T: KafkaDecoder](settings: Properties,
                                                     subscription: Subscription,
                                                     maxPendingCommits: Int,
                                                     partitionBufferSize: Int,
                                                     pollTimeout: FiniteDuration,
                                                     pollInterval: FiniteDuration)(
    implicit
    timer: Timer[F]): Resource[F, PartitionedConsumer[F, T]] =
    Resource
      .make(resources(settings, maxPendingCommits, partitionBufferSize, pollTimeout, pollInterval)) {
        case (_, _, shutdown, consumer, _) =>
          for {
            _ <- Sync[F].delay(consumer.unsubscribe())
            _ <- shutdown
            _ <- Sync[F].delay(consumer.close())
          } yield ()
      }
      .flatMap {
        case (commitQueue, partitionsOut, _, consumer, rebalanceListener) =>
          Resource.liftF {
            subscribe(consumer, subscription, Some(rebalanceListener))
              .as(PartitionedConsumer(commitQueue, partitionsOut.dequeue))
          }
      }
}
