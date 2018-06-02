package com.iravid.fs2.kafka

import cats.effect._, cats.implicits._
import fs2._
import java.util.{ Collection => JCollection, Properties }
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

case class PartitionedConsumer[F[_]](commitQueue: CommitQueue[F],
                                     records: Stream[F, (TopicPartition, Stream[F, ByteRecord])])

object PartitionedConsumer {
  import KafkaConsumerFunctions._

  case class PartitionHandle[F[_]](data: async.mutable.Queue[F, ByteRecord],
                                   interruption: async.Promise[F, Either[Throwable, Unit]]) {
    def records(implicit F: Effect[F], ec: ExecutionContext): Stream[F, ByteRecord] =
      data.dequeue.interruptWhen(interruption.get)
  }

  object PartitionHandle {
    def fromTopicPartition[F[_]: Effect](tp: TopicPartition, bufferSize: Int)(
      implicit ec: ExecutionContext): F[(TopicPartition, PartitionHandle[F])] =
      for {
        queue   <- async.boundedQueue[F, ByteRecord](bufferSize)
        promise <- async.Promise.empty[F, Either[Throwable, Unit]]
      } yield (tp, PartitionHandle(queue, promise))
  }

  def resources[F[_]: ConcurrentEffect](
    settings: Properties,
    maxPendingCommits: Int,
    partitionBufferSize: Int,
    pollTimeout: FiniteDuration,
    pollInterval: FiniteDuration
  )(implicit ec: ExecutionContext, timer: Timer[F]) =
    for {
      consumer         <- createConsumer[F](settings)
      commitQueue      <- CommitQueue.create[F](maxPendingCommits)
      partitionTracker <- async.boundedQueue[F, Map[TopicPartition, PartitionHandle[F]]](1)
      _                <- partitionTracker.enqueue1(Map())
      partitionsOut    <- async.unboundedQueue[F, (TopicPartition, Stream[F, ByteRecord])]

      rebalanceListener = new ConsumerRebalanceListener {
        def onPartitionsAssigned(partitions: JCollection[TopicPartition]): Unit = {
          val handler = for {
            handles <- partitions.asScala.toList
                        .traverse(PartitionHandle.fromTopicPartition[F](_, partitionBufferSize))
            _ <- partitionTracker.dequeue1
                  .map(_ ++ handles)
                  .flatMap(partitionTracker.enqueue1)
            _ <- handles.traverse {
                  case (tp, h) => partitionsOut.enqueue1((tp, h.records))
                }
          } yield ()

          Effect[F].runAsync(handler)(_ => IO.unit).unsafeRunSync()
        }

        def onPartitionsRevoked(jpartitions: JCollection[TopicPartition]): Unit = {
          val handler = for {
            tracker <- partitionTracker.dequeue1
            partitions = jpartitions.asScala.toList
            handles    = partitions.flatMap(tracker.get)
            _ <- handles.traverse_(_.interruption.complete(Right(())))
            _ <- partitionTracker.enqueue1(tracker -- partitions)
          } yield ()

          Effect[F].runAsync(handler)(_ => IO.unit).unsafeRunSync()
        }
      }

      commits = commitQueue.queue.dequeue
      polls   = Stream(Poll) ++ Stream.repeatEval(timer.sleep(pollInterval).as(Poll))

      pollingLoopShutdown <- async.Promise.empty[F, Either[Throwable, Unit]]

      _ <- Concurrent[F].start {
            commits
              .either(polls)
              .interruptWhen(pollingLoopShutdown.get)
              .evalMap {
                case Left(req) =>
                  (commit(consumer, req.asOffsetMap).void.attempt >>= req.promise.complete).void
                case Right(Poll) =>
                  for {
                    records <- poll(consumer, pollTimeout)
                    tracker <- partitionTracker.dequeue1
                    _ <- records.traverse_ { record =>
                          tracker
                            .get(new TopicPartition(record.topic, record.partition))
                            .traverse_(_.data.enqueue1(record))
                        }
                    _ <- partitionTracker.enqueue1(tracker)
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

  def apply[F[_]: ConcurrentEffect](settings: Properties,
                                    subscription: Subscription,
                                    maxPendingCommits: Int,
                                    partitionBufferSize: Int,
                                    pollTimeout: FiniteDuration,
                                    pollInterval: FiniteDuration)(
    implicit ec: ExecutionContext,
    timer: Timer[F]): F[PartitionedConsumer[F]] =
    Stream
      .bracket(
        resources(settings, maxPendingCommits, partitionBufferSize, pollTimeout, pollInterval))(
        {
          case (commitQueue, partitionsOut, _, consumer, rebalanceListener) =>
            Stream.eval {
              subscribe(consumer, subscription, Some(rebalanceListener))
                .as(PartitionedConsumer(commitQueue, partitionsOut.dequeue))
            }
        }, {
          case (_, _, shutdown, consumer, _) =>
            for {
              _ <- Sync[F].delay(consumer.unsubscribe())
              _ <- shutdown
              _ <- Sync[F].delay(consumer.close())
            } yield ()
        }
      )
      .compile
      .toList
      .map(_.last) // TODO: Replace with F.bracket when cats-effect 1.0 is out
}
