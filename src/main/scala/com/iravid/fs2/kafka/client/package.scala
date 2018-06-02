package com.iravid.fs2.kafka

import cats.Id
import java.util.{ Map => JMap }
import org.apache.kafka.clients.consumer.{ ConsumerRecord, KafkaConsumer, OffsetAndMetadata }
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord, RecordMetadata }
import org.apache.kafka.common.TopicPartition

package object client {
  type ByteRecord = ConsumerRecord[Array[Byte], Array[Byte]]
  type ByteConsumer = KafkaConsumer[Array[Byte], Array[Byte]]
  type OffsetMap = Map[TopicPartition, OffsetAndMetadata]
  type JOffsetMap = JMap[TopicPartition, OffsetAndMetadata]
  type ByteProducerRecord = ProducerRecord[Array[Byte], Array[Byte]]
  type ByteProducer = KafkaProducer[Array[Byte], Array[Byte]]

  type ConsumerMessage[F[_], A] = EnvT[ByteRecord, F, A]

  type ProducerResult[A] = EnvT[RecordMetadata, Id, A]

  type Result[A] = Either[Throwable, A]
}
