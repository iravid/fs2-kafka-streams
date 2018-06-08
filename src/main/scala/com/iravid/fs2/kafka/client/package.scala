package com.iravid.fs2.kafka

import java.util.{ Map => JMap }
import org.apache.kafka.clients.consumer.{ KafkaConsumer => JKafkaConsumer, OffsetAndMetadata }
import org.apache.kafka.clients.producer.{ KafkaProducer => JKafkaProducer }
import org.apache.kafka.common.TopicPartition

package object client {
  type ByteConsumer = JKafkaConsumer[Array[Byte], Array[Byte]]

  type ByteProducer = JKafkaProducer[Array[Byte], Array[Byte]]

  type OffsetMap = Map[TopicPartition, OffsetAndMetadata]
  type JOffsetMap = JMap[TopicPartition, OffsetAndMetadata]
}
