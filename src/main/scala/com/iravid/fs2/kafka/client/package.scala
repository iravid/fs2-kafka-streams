package com.iravid.fs2.kafka

import java.util.{ Map => JMap }
import org.apache.kafka.clients.consumer.{ KafkaConsumer, OffsetAndMetadata }
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition

package object client {
  type ByteConsumer = KafkaConsumer[Array[Byte], Array[Byte]]

  type ByteProducer = KafkaProducer[Array[Byte], Array[Byte]]

  type OffsetMap = Map[TopicPartition, OffsetAndMetadata]
  type JOffsetMap = JMap[TopicPartition, OffsetAndMetadata]
}
