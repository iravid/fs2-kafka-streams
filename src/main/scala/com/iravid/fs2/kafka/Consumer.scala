package com.iravid.fs2.kafka

import cats.effect.{ Concurrent, ConcurrentEffect }
import cats.implicits._
import fs2.{ async, Stream }
import java.util.Properties
import scala.concurrent.ExecutionContext

import scala.concurrent.duration.FiniteDuration

case class Consumer[F[_]](shutdown: F[Unit],
                          commitQueue: CommitQueue[F],
                          records: Stream[F, ByteRecord])

object Consumer {
  import KafkaConsumerFunctions._

  def apply[F[_]: ConcurrentEffect](
    settings: Properties,
    subscription: Subscription,
    maxPendingCommits: Int,
    bufferSize: Int,
    pollInterval: FiniteDuration,
    pollTimeout: FiniteDuration)(implicit ec: ExecutionContext): F[Consumer[F]] =
    for {
      consumer    <- createConsumer[F](settings)
      _           <- subscribe(consumer, subscription, None)
      commitQueue <- CommitQueue.create[F](maxPendingCommits)

      outQueue    <- async.boundedQueue[F, ByteRecord](bufferSize)
      shutdownSig <- async.Promise.empty[F, Either[Throwable, Unit]]
      records = outQueue.dequeue.interruptWhen(shutdownSig.get)

      commits = commitQueue.queue.dequeue
      polls   = Stream.every(pollInterval).as(Poll)
      consumerFiber <- Concurrent[F].start {
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

      interruptEverything = for {
        _ <- consumerFiber.cancel
        _ <- shutdownSig.complete(Right(()))
      } yield ()

    } yield Consumer(interruptEverything, commitQueue, records)
}
