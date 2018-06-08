package com.iravid.fs2.kafka.client

import cats.Id
import cats.effect.{ Async, Resource, Sync }
import com.iravid.fs2.kafka.EnvT
import com.iravid.fs2.kafka.codecs.KafkaEncoder
import com.iravid.fs2.kafka.model.{ ByteProducerRecord, ProducerResult }
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.producer.{ Callback, RecordMetadata }
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.serialization.ByteArraySerializer

import scala.collection.JavaConverters._

object Producer {
  def create[F[_]: Sync](settings: ProducerSettings): Resource[F, ByteProducer] =
    Resource.make(Sync[F].delay {
      new ByteProducer(settings.driverProperties, new ByteArraySerializer, new ByteArraySerializer)
    })(producer =>
      Sync[F].delay(producer.close(settings.closeTimeout.toMillis, TimeUnit.MILLISECONDS)))

  def toProducerRecord[T: KafkaEncoder](t: T,
                                        topic: String,
                                        partition: Int,
                                        timestamp: Option[Long]): ByteProducerRecord = {
    val (key, value) = KafkaEncoder[T].encode(t)

    new ByteProducerRecord(
      topic,
      partition,
      timestamp.map(new java.lang.Long(_)).orNull,
      key.map(_.data).orNull,
      value.data,
      List.empty[Header].asJava)
  }

  def produce[F[_]: Async, T: KafkaEncoder](producer: ByteProducer,
                                            data: T,
                                            topic: String,
                                            partition: Int,
                                            timestamp: Option[Long]): F[ProducerResult[T]] =
    Async[F].async { cb =>
      val record = toProducerRecord(data, topic, partition, timestamp)

      producer.send(
        record,
        new Callback {
          override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =
            if (exception eq null) cb(Right(EnvT[RecordMetadata, Id, T](metadata, data)))
            else cb(Left(exception))
        }
      )

      ()
    }
}
