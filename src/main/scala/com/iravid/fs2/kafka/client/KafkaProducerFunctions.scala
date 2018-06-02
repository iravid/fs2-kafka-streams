package com.iravid.fs2.kafka.client

import cats.effect.Async
import org.apache.kafka.clients.producer.{ Callback, RecordMetadata }

object Producing {
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
