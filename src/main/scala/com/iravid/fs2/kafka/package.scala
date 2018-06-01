package com.iravid.fs2

import java.util.{ Map => JMap, Collection => JCollection, Properties }
import org.apache.kafka.clients.consumer.{ ConsumerRecord, KafkaConsumer, OffsetAndMetadata }
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord }
import org.apache.kafka.common.TopicPartition

package object kafka {
  type ByteRecord = ConsumerRecord[Array[Byte], Array[Byte]]
  type ByteConsumer = KafkaConsumer[Array[Byte], Array[Byte]]
  type OffsetMap = Map[TopicPartition, OffsetAndMetadata]
  type JOffsetMap = JMap[TopicPartition, OffsetAndMetadata]
  type ByteProducerRecord = ProducerRecord[Array[Byte], Array[Byte]]
  type ByteProducer = KafkaProducer[Array[Byte], Array[Byte]]
}
