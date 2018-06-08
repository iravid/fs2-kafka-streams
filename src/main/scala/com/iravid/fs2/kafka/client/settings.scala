package com.iravid.fs2.kafka.client

import scala.concurrent.duration.FiniteDuration
import java.util.Properties

import scala.collection.JavaConverters._

case class ConsumerSettings(driverSettings: Map[String, String],
                            maxPendingCommits: Int,
                            outputBufferSize: Int,
                            partitionOutputBufferSize: Int,
                            pollTimeout: FiniteDuration,
                            pollInterval: FiniteDuration,
                            wakeupTimeout: FiniteDuration) {
  def driverProperties: Properties = {
    val props = new java.util.Properties()
    props.putAll(driverSettings.asJava)
    props
  }
}

case class ProducerSettings(driverSettings: Map[String, String], closeTimeout: FiniteDuration) {
  def driverProperties: Properties = {
    val props = new java.util.Properties()
    props.putAll(driverSettings.asJava)
    props
  }
}
