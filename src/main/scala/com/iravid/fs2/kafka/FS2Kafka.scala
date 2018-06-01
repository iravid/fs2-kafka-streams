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

object ConsumerCommands {
  case class CommitRequest[F[_]](promise: async.Promise[F, Either[Throwable, Unit]], topic: String,
    partition: Int, offset: Long) {
    def asOffsetMap: OffsetMap =
      Map(new TopicPartition(topic, partition) -> new OffsetAndMetadata(offset))
  }

  object CommitRequest {
    def apply[F[_]: Effect](topic: String, partition: Int, offset: Long)(
      implicit ec: ExecutionContext): F[CommitRequest[F]] =
      async.Promise.empty[F, Either[Throwable, Unit]]
        .map(CommitRequest(_, topic, partition, offset))
  }

  case object Poll
}

case class CommitQueue[F[_]](queue: async.mutable.Queue[F, ConsumerCommands.CommitRequest[F]]) {
  def requestCommit(request: ConsumerCommands.CommitRequest[F])(
    implicit F: MonadError[F, Throwable]): F[Unit] =
      queue.enqueue1(request) *> request.promise.get.rethrow
}

object CommitQueue {
  def create[F[_]: Effect](size: Int)(implicit ec: ExecutionContext) =
    async.boundedQueue[F, ConsumerCommands.CommitRequest[F]](size).map(CommitQueue(_))
}

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

case class ConsumerControl[F[_]](commitQueue: CommitQueue[F], records: Stream[F, ByteRecord])
case class PartitionedConsumerControl[F[_]](shutdown: F[Unit],
  records: Stream[F, (TopicPartition, Stream[F, ByteRecord])],
  commitQueue: CommitQueue[F])

sealed trait Subscription
object Subscription {
  case class Topics(topics: List[String]) extends Subscription
  case class Pattern(pattern: String) extends Subscription
}

object Consuming {
  def createConsumer[F[_]: Sync](settings: Properties): F[ByteConsumer] =
    Sync[F].delay(new ByteConsumer(settings))

  def consumer[F[_]: Effect](settings: Properties, subscription: Subscription,
    maxPendingCommits: Int, pollInterval: FiniteDuration, pollTimeout: FiniteDuration)(
    implicit ec: ExecutionContext): F[ConsumerControl[F]] =
    for {
      consumer <- createConsumer[F](settings)
      _ <- subscribe(consumer, subscription, None)
      commitQueue <- CommitQueue.create[F](maxPendingCommits)
      commits = commitQueue.queue.dequeue
      polls = Stream.every(pollInterval).as(ConsumerCommands.Poll)
      res = commits.either(polls)
              .evalMap {
                case Left(req) =>
                  (commit(consumer, req.asOffsetMap).void.attempt >>= req.promise.complete)
                    .as(List.empty[ByteRecord])
                case Right(ConsumerCommands.Poll) =>
                  poll(consumer, pollTimeout)
              }
              .flatMap(Stream.emits(_))
    } yield ConsumerControl(commitQueue, res)

  def commit[F[_]: Async](consumer: ByteConsumer, data: OffsetMap): F[OffsetMap] =
    Async[F].async { cb =>
      consumer.commitAsync(data.asJava, new OffsetCommitCallback {
        override def onComplete(metadata: JOffsetMap, exception: Exception): Unit =
          if (exception eq null) cb(Right(metadata.asScala.toMap))
          else cb(Left(exception))
      })
    }

  def poll[F[_]: Sync](consumer: ByteConsumer, timeout: FiniteDuration): F[List[ByteRecord]] =
    Sync[F].delay(consumer.poll(timeout.toMillis).iterator().asScala.toList)

  def subscribe[F[_]: Sync](consumer: ByteConsumer, subscription: Subscription,
    rebalanceListener: Option[ConsumerRebalanceListener]): F[Unit] = {
      val listener = rebalanceListener.getOrElse(new NoOpConsumerRebalanceListener)

      subscription match {
        case Subscription.Topics(topics) =>
          Sync[F].delay(consumer.subscribe(topics.asJava, listener))
        case Subscription.Pattern(pattern) =>
          for {
            pattern <- Sync[F].delay(java.util.regex.Pattern.compile(pattern))
            _ <- Sync[F].delay(consumer.subscribe(pattern, listener))
          } yield ()
      }
    }

  def partitionedConsumer[F[_]: ConcurrentEffect](settings: Properties,
    subscription: Subscription, maxPendingCommits: Int, pollTimeout: FiniteDuration,
    pollInterval: FiniteDuration)(implicit ec: ExecutionContext) =
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
      polls = Stream.every(pollInterval).as(ConsumerCommands.Poll)
      consumerDriver = commits.either(polls)
        .evalMap {
          case Left(req) =>
            (commit(consumer, req.asOffsetMap).void.attempt >>= req.promise.complete)
              .void
          case Right(ConsumerCommands.Poll) =>
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
    } yield PartitionedConsumerControl(interruptEverything, out.dequeue, commitQueue)
}

object Producing {
  def produce[F[_]: Async](producer: ByteProducer, record: ByteProducerRecord): F[RecordMetadata] =
    Async[F].async { cb =>
      producer.send(record, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =
          if (exception eq null) cb(Right(metadata))
          else cb(Left(exception))
      })
    }
}
