package com.iravid.fs2.kafka.client

import cats.effect.Concurrent
import cats.effect.concurrent.Deferred
import cats.implicits._
import fs2.async

case class CommitQueue[F[_]](
  queue: async.mutable.Queue[F, (Deferred[F, Either[Throwable, Unit]], CommitRequest)]) {
  def requestCommit(request: CommitRequest)(implicit F: Concurrent[F]): F[Unit] =
    for {
      deferred <- Deferred[F, Either[Throwable, Unit]]
      _        <- queue.enqueue1((deferred, request))
      _        <- deferred.get.rethrow
    } yield ()
}

object CommitQueue {
  def create[F[_]: Concurrent](size: Int) =
    async
      .boundedQueue[F, (Deferred[F, Either[Throwable, Unit]], CommitRequest)](size)
      .map(CommitQueue(_))
}
