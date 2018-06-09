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
  def resources[F[_], T: KafkaDecoder](settings: ConsumerSettings, consumer: Consumer[F])(
    implicit F: ConcurrentEffect[F],
    timer: Timer[F]) =
    for {
      commitQueue         <- CommitQueue.create[F](settings.maxPendingCommits)
      outQueue            <- async.boundedQueue[F, Option[ByteRecord]](settings.outputBufferSize)
      pollingLoopShutdown <- Deferred[F, Either[Throwable, Unit]]
      shutdownQueue       <- async.boundedQueue[F, None.type](1)
      commits       = commitQueue.queue.dequeue
      polls         = Stream(Poll) ++ Stream.repeatEval(timer.sleep(settings.pollInterval).as(Poll))
      commandStream = shutdownQueue.dequeue.merge(commits.either(polls).map(_.some))
      pollingLoop <- Concurrent[F].start {
                      commandStream.unNoneTerminate
                        .evalMap {
                          case Left((deferred, req)) =>
                            (consumer
                              .commit(req.asOffsetMap)
                              .void
                              .attempt >>= deferred.complete).void
                          case Right(Poll) =>
                            for {
                              records <- consumer.poll(settings.pollTimeout, settings.wakeupTimeout)
                              _       <- records.traverse_(rec => outQueue.enqueue1(rec.some))
                            } yield ()
                        }
                        .compile
                        .drain
                    }
      outputStream = outQueue.dequeue.unNoneTerminate
        .through(deserialize[F, T])
      performShutdown = shutdownQueue.enqueue1(None) *>
        outQueue.enqueue1(None) *>
        pollingLoop.join
    } yield (commitQueue, outputStream, performShutdown)

  def apply[F[_]: ConcurrentEffect, T: KafkaDecoder](
    settings: ConsumerSettings,
    consumer: Consumer[F],
    subscription: Subscription)(implicit timer: Timer[F]): Resource[F, RecordStream[F, T]] =
    Resource.make(resources[F, T](settings, consumer)) {
      case (_, _, shutdown) =>
        for {
          _ <- shutdown
          _ <- consumer.unsubscribe
        } yield ()
    } flatMap {
      case (commitQueue, outputStream, _) =>
        Resource.liftF {
          consumer
            .subscribe(subscription, _ => Sync[F].unit)
            .as(RecordStream(commitQueue, outputStream))
        }
    }

  def deserialize[F[_], T: KafkaDecoder]: Pipe[F, ByteRecord, ConsumerMessage[Result, T]] =
    _.map(rec => EnvT(rec, KafkaDecoder[T].decode(rec)))
}
