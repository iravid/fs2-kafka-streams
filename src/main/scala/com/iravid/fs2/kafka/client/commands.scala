package com.iravid.fs2.kafka.client

import cats.effect.Effect
import cats.implicits._
import fs2.async
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import scala.concurrent.ExecutionContext

case class CommitRequest[F[_]](promise: async.Promise[F, Either[Throwable, Unit]],
                               topic: String,
                               partition: Int,
                               offset: Long) {
  def asOffsetMap: OffsetMap =
    Map(new TopicPartition(topic, partition) -> new OffsetAndMetadata(offset))
}

object CommitRequest {
  def apply[F[_]: Effect](topic: String, partition: Int, offset: Long)(
    implicit ec: ExecutionContext): F[CommitRequest[F]] =
    async.Promise
      .empty[F, Either[Throwable, Unit]]
      .map(CommitRequest(_, topic, partition, offset))
}

case object Poll
