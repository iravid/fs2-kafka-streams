package com.iravid.fs2.kafka.codecs

import com.iravid.fs2.kafka.model.{ ByteRecord, Result }
import cats.MonadError
import scala.annotation.tailrec

object KafkaDecoder {
  def apply[T: KafkaDecoder]: KafkaDecoder[T] = implicitly

  def instance[T](f: ByteRecord => Result[T]): KafkaDecoder[T] =
    new KafkaDecoder[T] {
      def decode(record: ByteRecord) = f(record)
    }

  implicit val monad: MonadError[KafkaDecoder, Throwable] =
    new MonadError[KafkaDecoder, Throwable] {
      def pure[A](a: A): KafkaDecoder[A] = instance(_ => Right(a))

      def handleErrorWith[A](fa: KafkaDecoder[A])(f: Throwable => KafkaDecoder[A]) =
        instance { record =>
          fa.decode(record).fold(e => f(e).decode(record), Right(_))
        }

      def raiseError[A](e: Throwable): KafkaDecoder[A] = instance(_ => Left(e))

      def flatMap[A, B](fa: KafkaDecoder[A])(f: A => KafkaDecoder[B]): KafkaDecoder[B] =
        instance { record =>
          fa.decode(record).flatMap(f(_).decode(record))
        }

      def tailRecM[A, B](a: A)(f: A => KafkaDecoder[Either[A, B]]): KafkaDecoder[B] = {
        @tailrec
        def go(data: ByteRecord, a: A): Result[B] =
          f(a).decode(data) match {
            case Right(Left(a))  => go(data, a)
            case Right(Right(b)) => Right(b)
            case Left(e)         => Left(e)
          }

        instance { record =>
          go(record, a)
        }
      }
    }

  implicit val decoderForByteRecord: KafkaDecoder[ByteRecord] =
    instance(Right(_))
}

trait KafkaDecoder[T] {
  def decode(record: ByteRecord): Result[T]
}
