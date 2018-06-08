package com.iravid.fs2.kafka.client

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

case class CommitRequest(topic: String, partition: Int, offset: Long) {
  def asOffsetMap: OffsetMap =
    Map(new TopicPartition(topic, partition) -> new OffsetAndMetadata(offset))
}

case object Poll

sealed trait Rebalance
object Rebalance {
  type Listener[F[_]] = Rebalance => F[Unit]

  case class Assign(partitions: List[TopicPartition]) extends Rebalance
  case class Revoke(partitions: List[TopicPartition]) extends Rebalance
}
