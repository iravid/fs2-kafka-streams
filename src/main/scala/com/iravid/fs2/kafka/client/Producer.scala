package com.iravid.fs2.kafka.client

import cats.effect.{ Async, Sync }
import fs2.Stream
import java.util.Properties
import org.apache.kafka.clients.producer.{ Callback, RecordMetadata }
import org.apache.kafka.common.serialization.ByteArraySerializer

object Producer {
  def create[F[_]: Sync](settings: Properties) =
    Stream.bracket(Sync[F].delay {
      new ByteProducer(settings, new ByteArraySerializer, new ByteArraySerializer)
    })(Stream.emit(_), producer => Sync[F].delay(producer.close()))

  def produce[F[_]: Async](producer: ByteProducer, record: ByteProducerRecord): F[RecordMetadata] =
    Async[F].async { cb =>
      producer.send(
        record,
        new Callback {
          override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =
            if (exception eq null) cb(Right(metadata))
            else cb(Left(exception))
        }
      )

      ()
    }
}
