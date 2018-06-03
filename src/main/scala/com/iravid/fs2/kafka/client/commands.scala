package com.iravid.fs2.kafka.client

import cats.effect.Concurrent
import cats.effect.concurrent.Deferred
import cats.implicits._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

case class CommitRequest[F[_]](promise: Deferred[F, Either[Throwable, Unit]],
                               topic: String,
                               partition: Int,
                               offset: Long) {
  def asOffsetMap: OffsetMap =
    Map(new TopicPartition(topic, partition) -> new OffsetAndMetadata(offset))
}

object CommitRequest {
  def apply[F[_]: Concurrent](topic: String, partition: Int, offset: Long): F[CommitRequest[F]] =
    Deferred[F, Either[Throwable, Unit]]
      .map(CommitRequest(_, topic, partition, offset))
}

case object Poll
