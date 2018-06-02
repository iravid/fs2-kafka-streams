package com.iravid.fs2.kafka

import cats.effect.{ Concurrent, ConcurrentEffect, Sync }
import cats.implicits._
import fs2.{ async, Stream }
import java.util.Properties
import scala.concurrent.ExecutionContext

import scala.concurrent.duration.FiniteDuration

case class Consumer[F[_]](commitQueue: CommitQueue[F], records: Stream[F, ByteRecord])

object Consumer {
  import KafkaConsumerFunctions._

  def resources[F[_]: ConcurrentEffect](
    settings: Properties,
    maxPendingCommits: Int,
    bufferSize: Int,
    pollInterval: FiniteDuration,
    pollTimeout: FiniteDuration)(implicit ec: ExecutionContext) =
    for {
      consumer    <- createConsumer[F](settings)
      commitQueue <- CommitQueue.create[F](maxPendingCommits)
      outQueue    <- async.boundedQueue[F, ByteRecord](bufferSize)
      commits = commitQueue.queue.dequeue
      polls   = Stream.every(pollInterval).as(Poll)
      driver <- Concurrent[F].start {
                 commits
                   .either(polls)
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
    } yield (commitQueue, outQueue, driver, consumer)

  def apply[F[_]: ConcurrentEffect](
    settings: Properties,
    subscription: Subscription,
    maxPendingCommits: Int,
    bufferSize: Int,
    pollInterval: FiniteDuration,
    pollTimeout: FiniteDuration)(implicit ec: ExecutionContext): F[Consumer[F]] =
    Stream
      .bracket(resources[F](settings, maxPendingCommits, bufferSize, pollInterval, pollTimeout))(
        {
          case (commitQueue, outQueue, _, consumer) =>
            Stream.eval {
              subscribe(consumer, subscription, None)
                .as(Consumer(commitQueue, outQueue.dequeue))
            }
        }, {
          case (_, _, driver, consumer) =>
            for {
              _ <- Sync[F].delay(consumer.unsubscribe())
              _ <- driver.cancel
              _ <- Sync[F].delay(consumer.close())
            } yield ()
        }
      )
      .compile
      .toList
      .map(_.last)

}
