package com.iravid.fs2.kafka.client

import org.apache.kafka.common.TopicPartition

sealed trait Rebalance
object Rebalance {
  type Listener[F[_]] = Rebalance => F[Unit]

  case class Assign(partitions: List[TopicPartition]) extends Rebalance
  case class Revoke(partitions: List[TopicPartition]) extends Rebalance
}
