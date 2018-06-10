package com.iravid.fs2.kafka.client

import cats.kernel.{ Monoid, Semigroup }
import cats.implicits._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

case class CommitRequest(offsets: OffsetMap)
object CommitRequest {
  def apply(topic: String, partition: Int, offset: Long): CommitRequest =
    CommitRequest(Map(new TopicPartition(topic, partition) -> new OffsetAndMetadata(offset)))

  implicit val offsetAndMetadataSemigroup: Semigroup[OffsetAndMetadata] =
    Semigroup.instance { (x, y) =>
      if (x.offset >= y.offset) x
      else y
    }

  implicit val commitRequestMonoid: Monoid[CommitRequest] =
    new Monoid[CommitRequest] {
      def empty: CommitRequest = CommitRequest(Map.empty)
      def combine(x: CommitRequest, y: CommitRequest): CommitRequest =
        CommitRequest(x.offsets |+| y.offsets)
    }
}

case object Poll

sealed trait Rebalance
object Rebalance {
  type Listener[F[_]] = Rebalance => F[Unit]

  case class Assign(partitions: List[TopicPartition]) extends Rebalance
  case class Revoke(partitions: List[TopicPartition]) extends Rebalance
}
