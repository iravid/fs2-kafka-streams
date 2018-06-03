package com.iravid.fs2.kafka.client

import cats.MonadError
import cats.effect.Concurrent
import cats.implicits._
import fs2.async

case class CommitQueue[F[_]](queue: async.mutable.Queue[F, CommitRequest[F]]) {
  def requestCommit(request: CommitRequest[F])(implicit F: MonadError[F, Throwable]): F[Unit] =
    queue.enqueue1(request) *> request.promise.get.rethrow
}

object CommitQueue {
  def create[F[_]: Concurrent](size: Int) =
    async.boundedQueue[F, CommitRequest[F]](size).map(CommitQueue(_))
}
