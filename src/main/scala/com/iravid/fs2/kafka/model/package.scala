package com.iravid.fs2.kafka

import cats.Id
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{ ProducerRecord, RecordMetadata }

package object model {
  type ByteRecord = ConsumerRecord[Array[Byte], Array[Byte]]

  type ByteProducerRecord = ProducerRecord[Array[Byte], Array[Byte]]

  type ConsumerMessage[F[_], A] = EnvT[ByteRecord, F, A]

  type ProducerResult[A] = EnvT[RecordMetadata, Id, A]

  type Result[A] = Either[Throwable, A]
}
