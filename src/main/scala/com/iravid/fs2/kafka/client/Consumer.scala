package com.iravid.fs2.kafka.client

import cats.effect.Resource
import cats.effect.concurrent.Deferred
import cats.effect.{ Concurrent, ConcurrentEffect, Sync, Timer }
import cats.implicits._
import com.iravid.fs2.kafka.codecs.KafkaDecoder
import com.iravid.fs2.kafka.model.{ ByteRecord, ConsumerMessage, Result }
import fs2.{ async, Stream }
import java.util.Properties

import scala.concurrent.duration.FiniteDuration

case class Consumer[F[_], T](commitQueue: CommitQueue[F],
                             records: Stream[F, ConsumerMessage[Result, T]])

object Consumer {
  import KafkaConsumerFunctions._

  def resources[F[_]: ConcurrentEffect](settings: Properties,
                                        maxPendingCommits: Int,
                                        bufferSize: Int,
                                        pollInterval: FiniteDuration,
                                        pollTimeout: FiniteDuration,
                                        wakeupTimeout: FiniteDuration)(implicit timer: Timer[F]) =
    for {
      consumer            <- createConsumer[F](settings)
      commitQueue         <- CommitQueue.create[F](maxPendingCommits)
      outQueue            <- async.boundedQueue[F, ByteRecord](bufferSize)
      pollingLoopShutdown <- Deferred[F, Either[Throwable, Unit]]
      commits = commitQueue.queue.dequeue
      polls   = Stream(Poll) ++ Stream.repeatEval(timer.sleep(pollInterval).as(Poll))
      _ <- Concurrent[F].start {
            commits
              .either(polls)
              .interruptWhen(pollingLoopShutdown.get)
              .evalMap {
                case Left(req) =>
                  (commit(consumer, req.asOffsetMap).void.attempt >>= req.promise.complete).void
                case Right(Poll) =>
                  for {
                    records <- poll(consumer, pollTimeout, wakeupTimeout)
                    _       <- records.traverse_(outQueue.enqueue1)
                  } yield ()
              }
              .compile
              .drain
          }
    } yield (commitQueue, outQueue, pollingLoopShutdown.complete(Right(())), consumer)

  def apply[F[_]: ConcurrentEffect, T: KafkaDecoder](
    settings: Properties,
    subscription: Subscription,
    maxPendingCommits: Int,
    bufferSize: Int,
    pollInterval: FiniteDuration,
    pollTimeout: FiniteDuration,
    wakeupTimeout: FiniteDuration)(implicit timer: Timer[F]): Resource[F, Consumer[F, T]] = {
    val res = resources[F](
      settings,
      maxPendingCommits,
      bufferSize,
      pollInterval,
      pollTimeout,
      wakeupTimeout)

    Resource.make(res) {
      case (_, _, shutdown, consumer) =>
        for {
          _ <- Sync[F].delay(consumer.unsubscribe())
          _ <- shutdown
          _ <- Sync[F].delay(consumer.close())
        } yield ()
    } flatMap {
      case (commitQueue, outQueue, _, consumer) =>
        Resource.liftF {
          subscribe(consumer, subscription, None)
            .as(Consumer(commitQueue, outQueue.dequeue through deserialize[F, T]))
        }
    }
  }
}
