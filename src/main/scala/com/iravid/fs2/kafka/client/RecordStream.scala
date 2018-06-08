package com.iravid.fs2.kafka.client

import cats.effect.Resource
import cats.effect.concurrent.Deferred
import cats.effect.{ Concurrent, ConcurrentEffect, Sync, Timer }
import cats.implicits._
import com.iravid.fs2.kafka.EnvT
import com.iravid.fs2.kafka.codecs.KafkaDecoder
import com.iravid.fs2.kafka.model.{ ByteRecord, ConsumerMessage, Result }
import fs2.{ async, Pipe, Stream }

case class RecordStream[F[_], T](commitQueue: CommitQueue[F],
                                 records: Stream[F, ConsumerMessage[Result, T]])

object RecordStream {
  def resources[F[_]: ConcurrentEffect](settings: ConsumerSettings)(implicit timer: Timer[F],
                                                                    consumer: Consumer[F]) =
    for {
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
                  (consumer.commit(req.asOffsetMap).void.attempt >>= req.promise.complete).void
                case Right(Poll) =>
                  for {
                    records <- consumer.poll(settings.pollTimeout, settings.wakeupTimeout)
                    _       <- records.traverse_(outQueue.enqueue1)
                  } yield ()
              }
              .compile
              .drain
          }
    } yield (commitQueue, outQueue, pollingLoopShutdown.complete(Right(())))

  def apply[F[_]: ConcurrentEffect, T: KafkaDecoder](settings: ConsumerSettings,
                                                     subscription: Subscription)(
    implicit timer: Timer[F],
    consumer: Consumer[F]): Resource[F, RecordStream[F, T]] =
    Resource.make(resources[F](settings)) {
      case (_, _, shutdown) =>
        for {
          _ <- consumer.unsubscribe
          _ <- shutdown
        } yield ()
    } flatMap {
      case (commitQueue, outQueue, _) =>
        Resource.liftF {
          consumer
            .subscribe(subscription, _ => Sync[F].unit)
            .as(RecordStream(commitQueue, outQueue.dequeue through deserialize[F, T]))
        }
    }

  def deserialize[F[_], T: KafkaDecoder]: Pipe[F, ByteRecord, ConsumerMessage[Result, T]] =
    _.map(rec => EnvT(rec, KafkaDecoder[T].decode(rec)))
}
