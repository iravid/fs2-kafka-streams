package com.iravid.fs2.kafka.client

import cats.effect.IO
import cats.implicits._
import com.iravid.fs2.kafka.codecs.{ KafkaDecoder, KafkaEncoder }
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.scalacheck.{ Gen, Shrink }
import org.scalatest.Suite

import scala.concurrent.duration._

trait KafkaSettings extends EmbeddedKafka { self: Suite =>
  def kafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)

  def mkConsumerSettings(port: Int, groupId: String, bufferSize: Int) = ConsumerSettings(
    Map(
      "bootstrap.servers" -> s"localhost:$port",
      "group.id"          -> groupId,
      "auto.offset.reset" -> "earliest"
    ),
    10,
    bufferSize,
    bufferSize,
    50.millis,
    50.millis,
    10.seconds
  )

  def mkProducerSettings(port: Int) = ProducerSettings(
    Map(
      "bootstrap.servers" -> s"localhost:${port}",
    ),
    5.seconds
  )

  implicit def decoder: KafkaDecoder[String] =
    KafkaDecoder.instance(rec => Right(new String(rec.value, "UTF-8")))

  implicit def encoder: KafkaEncoder[String] =
    KafkaEncoder.instance(str => (None, KafkaEncoder.Value(str.getBytes)))

  def produce(settings: ProducerSettings, topic: String, data: List[(Int, String)]) =
    Producer.create[IO](settings) use { producer =>
      data traverse {
        case (partition, msg) =>
          Producer.produce[IO, String](producer, msg, topic, partition, None)
      }
    }

  def nonEmptyStr = Gen.nonEmptyListOf(Gen.alphaLowerChar).map(_.mkString)

  implicit def noShrink[T]: Shrink[T] = Shrink.shrinkAny
}
