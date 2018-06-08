package com.iravid.fs2.kafka.client

import cats.effect.Resource
import cats.effect.concurrent.Deferred
import cats.effect.{ Concurrent, ConcurrentEffect, Sync, Timer }
import cats.implicits._
import com.iravid.fs2.kafka.codecs.KafkaDecoder
import com.iravid.fs2.kafka.model.{ ByteRecord, ConsumerMessage, Result }
import fs2.{ async, Stream }

case class Consumer[F[_], T](commitQueue: CommitQueue[F],
                             records: Stream[F, ConsumerMessage[Result, T]])

object Consumer {
  import KafkaConsumerFunctions._

  def resources[F[_]: ConcurrentEffect](settings: ConsumerSettings)(implicit timer: Timer[F]) =
    for {
      consumer            <- createConsumer[F](settings.driverProperties)
      commitQueue         <- CommitQueue.create[F](settings.maxPendingCommits)
      outQueue            <- async.boundedQueue[F, ByteRecord](settings.outputBufferSize)
      pollingLoopShutdown <- Deferred[F, Either[Throwable, Unit]]
      commits = commitQueue.queue.dequeue
      polls   = Stream(Poll) ++ Stream.repeatEval(timer.sleep(settings.pollInterval).as(Poll))
      _ <- Concurrent[F].start {
            commits
              .either(polls)
              .interruptWhen(pollingLoopShutdown.get)
              .evalMap {
                case Left(req) =>
                  (commit(consumer, req.asOffsetMap).void.attempt >>= req.promise.complete).void
                case Right(Poll) =>
                  for {
                    records <- poll(consumer, settings.pollTimeout, settings.wakeupTimeout)
                    _       <- records.traverse_(outQueue.enqueue1)
                  } yield ()
              }
              .compile
              .drain
          }
    } yield (commitQueue, outQueue, pollingLoopShutdown.complete(Right(())), consumer)

  def apply[F[_]: ConcurrentEffect, T: KafkaDecoder](
    settings: ConsumerSettings,
    subscription: Subscription)(implicit timer: Timer[F]): Resource[F, Consumer[F, T]] =
    Resource.make(resources[F](settings)) {
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
