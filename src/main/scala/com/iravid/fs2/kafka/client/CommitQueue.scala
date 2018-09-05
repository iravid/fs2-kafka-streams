package com.iravid.fs2.kafka.client

import cats.effect.Concurrent
import cats.effect.concurrent.Deferred
import cats.implicits._
import fs2.concurrent.Queue
import fs2.Stream

case class CommitQueue[F[_]](queue: Queue[F, (Deferred[F, Either[Throwable, Unit]], CommitRequest)],
                             batchSize: Int) {
  def requestCommit(request: CommitRequest)(implicit F: Concurrent[F]): F[Unit] =
    for {
      deferred <- Deferred[F, Either[Throwable, Unit]]
      _        <- queue.enqueue1((deferred, request))
      _        <- deferred.get.rethrow
    } yield ()

  def batchedDequeue(
    implicit F: Concurrent[F]): Stream[F, (Deferred[F, Either[Throwable, Unit]], CommitRequest)] =
    Stream
      .repeatEval(queue.dequeueBatch1(batchSize))
      .evalMap { chunk =>
        val (defs, reqs) = chunk.toList.unzip

        Deferred[F, Either[Throwable, Unit]].map { deferred =>
          (new Deferred[F, Either[Throwable, Unit]] {
            def get = deferred.get
            def complete(a: Either[Throwable, Unit]) =
              defs.traverse_(_.complete(a))
          }, reqs.foldMap(identity))
        }
      }
}

object CommitQueue {
  def create[F[_]: Concurrent](size: Int, batchSize: Int) =
    Queue
      .bounded[F, (Deferred[F, Either[Throwable, Unit]], CommitRequest)](size)
      .map(CommitQueue(_, batchSize))
}
