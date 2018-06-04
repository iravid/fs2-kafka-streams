package com.iravid.fs2.kafka.codecs

import cats.Contravariant

object KafkaEncoder {
  def apply[T: KafkaEncoder]: KafkaEncoder[T] = implicitly

  def instance[T](f: T => (Option[Key], Value)): KafkaEncoder[T] =
    new KafkaEncoder[T] {
      def encode(t: T) = f(t)
    }

  implicit val contravariant: Contravariant[KafkaEncoder] = new Contravariant[KafkaEncoder] {
    def contramap[A, B](fa: KafkaEncoder[A])(f: B => A): KafkaEncoder[B] =
      instance { b =>
        fa.encode(f(b))
      }
  }

  case class Key(data: Array[Byte]) extends AnyVal
  case class Value(data: Array[Byte]) extends AnyVal
}

trait KafkaEncoder[T] {
  import KafkaEncoder._

  def encode(t: T): (Option[Key], Value)
}
