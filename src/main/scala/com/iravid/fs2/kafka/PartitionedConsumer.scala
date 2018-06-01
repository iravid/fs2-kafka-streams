package com.iravid.fs2.kafka

import cats.MonadError
import cats.effect._, cats.effect.implicits._, cats.implicits._
import fs2._
import java.util.{ Collection => JCollection, Properties }
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
import org.apache.kafka.clients.consumer.{ ConsumerRebalanceListener, OffsetCommitCallback }
import org.apache.kafka.clients.producer.{ Callback, RecordMetadata }
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

case class PartitionedConsumer[F[_]](shutdown: F[Unit],
  records: Stream[F, (TopicPartition, Stream[F, ByteRecord])],
  commitQueue: CommitQueue[F])

object PartitionedConsumer {
  import KafkaConsumerFunctions._

  case class PartitionHandle[F[_]](data: async.mutable.Queue[F, ByteRecord], interruption: async.Promise[F, Either[Throwable, Unit]]) {
    def records(implicit F: Effect[F], ec: ExecutionContext): Stream[F, ByteRecord] =
      data.dequeue.interruptWhen(interruption.get)
  }

  object PartitionHandle {
    def fromTopicPartition[F[_]: Effect](tp: TopicPartition)(
      implicit ec: ExecutionContext): F[(TopicPartition, PartitionHandle[F])] =
      for {
        queue <- async.boundedQueue[F, ByteRecord](1000)
        promise <- async.Promise.empty[F, Either[Throwable, Unit]]
      } yield (tp, PartitionHandle(queue, promise))
  }

  def apply[F[_]: ConcurrentEffect](settings: Properties,
    subscription: Subscription, maxPendingCommits: Int, pollTimeout: FiniteDuration,
    pollInterval: FiniteDuration)(implicit ec: ExecutionContext): F[PartitionedConsumer[F]] =
    for {
      out <- async.unboundedQueue[F, (TopicPartition, Stream[F, ByteRecord])]
      commitQueue <- CommitQueue.create[F](maxPendingCommits)
      partitionTracker <- async.Ref[F, Map[TopicPartition, PartitionHandle[F]]](Map())
      consumer <- createConsumer[F](settings)
      _ <- subscribe(consumer, subscription, Some(new ConsumerRebalanceListener {
          def onPartitionsAssigned(partitions: JCollection[TopicPartition]): Unit = {
            val handler = for {
              handles <- partitions.asScala.toList.traverse(PartitionHandle.fromTopicPartition[F])
              _ <- partitionTracker.modify(_ ++ handles)
              _ <- handles.traverse { case (tp, h) => out.enqueue1((tp, h.records)) }
            } yield ()

            Effect[F].runAsync(handler)(_ => IO.unit).unsafeRunSync()
          }

          def onPartitionsRevoked(partitions: JCollection[TopicPartition]): Unit = {
            val handler = for {
              tracker <- partitionTracker.get
              handles = partitions.asScala.toList.flatMap(tracker.get)
              _ <- handles.traverse_(_.interruption.complete(Right(())))
            } yield ()

            Effect[F].runAsync(handler)(_ => IO.unit).unsafeRunSync()
          }
        }))

      commits = commitQueue.queue.dequeue
      polls = Stream.every(pollInterval).as(Poll)
      consumerDriver = commits.either(polls)
        .evalMap {
          case Left(req) =>
            (commit(consumer, req.asOffsetMap).void.attempt >>= req.promise.complete)
              .void
          case Right(Poll) =>
            for {
              records <- poll(consumer, pollTimeout)
              tracker <- partitionTracker.get
              _ <- records.traverse_ { record =>
                     tracker
                       .get(new TopicPartition(record.topic, record.partition))
                       .traverse_(_.data.enqueue1(record))
                   }
            } yield ()
        }
        .compile
        .drain
      fiber <- Concurrent[F].start(consumerDriver)
      interruptEverything = for {
        _ <- fiber.cancel
        tracker <- partitionTracker.get
        _ <- tracker.values.toList.traverse(_.interruption.complete(Right(())))
      } yield ()
    } yield PartitionedConsumer(interruptEverything, out.dequeue, commitQueue)
}
