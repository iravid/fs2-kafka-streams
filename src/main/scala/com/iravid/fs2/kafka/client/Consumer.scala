package com.iravid.fs2.kafka.client

import cats.effect.{ Concurrent, ConcurrentEffect, Sync, Timer }
import cats.implicits._
import com.iravid.fs2.kafka.client.codecs.KafkaDecoder
import fs2.{ async, Stream }
import java.util.Properties
import scala.concurrent.ExecutionContext

import scala.concurrent.duration.FiniteDuration

case class Consumer[F[_], T](commitQueue: CommitQueue[F],
                             records: Stream[F, ConsumerMessage[Result, T]])

object Consumer {
  import KafkaConsumerFunctions._

  def resources[F[_]: ConcurrentEffect](
    settings: Properties,
    maxPendingCommits: Int,
    bufferSize: Int,
    pollInterval: FiniteDuration,
    pollTimeout: FiniteDuration)(implicit ec: ExecutionContext, timer: Timer[F]) =
    for {
      consumer            <- createConsumer[F](settings)
      commitQueue         <- CommitQueue.create[F](maxPendingCommits)
      outQueue            <- async.boundedQueue[F, ByteRecord](bufferSize)
      pollingLoopShutdown <- async.Promise.empty[F, Either[Throwable, Unit]]
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
                    records <- poll(consumer, pollTimeout)
                    _       <- records.traverse_(outQueue.enqueue1)
                  } yield ()
              }
              .compile
              .drain
          }
    } yield (commitQueue, outQueue, pollingLoopShutdown.complete(Right(())), consumer)

  def apply[F[_]: ConcurrentEffect, T: KafkaDecoder](settings: Properties,
                                                     subscription: Subscription,
                                                     maxPendingCommits: Int,
                                                     bufferSize: Int,
                                                     pollInterval: FiniteDuration,
                                                     pollTimeout: FiniteDuration)(
    implicit ec: ExecutionContext,
    timer: Timer[F]): Stream[F, Consumer[F, T]] =
    Stream
      .bracket(resources[F](settings, maxPendingCommits, bufferSize, pollInterval, pollTimeout))(
        {
          case (commitQueue, outQueue, _, consumer) =>
            Stream.eval {
              subscribe(consumer, subscription, None)
                .as(Consumer(commitQueue, outQueue.dequeue through deserialize[F, T]))
            }
        }, {
          case (_, _, shutdown, consumer) =>
            for {
              _ <- Sync[F].delay(consumer.unsubscribe())
              _ <- shutdown
              _ <- Sync[F].delay(consumer.close())
            } yield ()
        }
      )
}
