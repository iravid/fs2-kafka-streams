package com.iravid.fs2.kafka.client

import cats.effect.{ IO, Resource }
import cats.implicits._
import com.iravid.fs2.kafka.codecs.{ KafkaDecoder, KafkaEncoder }
import org.apache.kafka.clients.admin.{ AdminClient, AdminClientConfig, NewTopic }
import org.scalacheck.{ Gen, Shrink }
import org.scalatest.Suite

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

trait KafkaSettings { self: Suite =>
  def mkConsumerSettings(groupId: String, bufferSize: Int) = ConsumerSettings(
    Map(
      "bootstrap.servers" -> s"localhost:9092",
      "group.id"          -> groupId,
      "auto.offset.reset" -> "earliest"
    ),
    10,
    10,
    bufferSize,
    bufferSize,
    50.millis,
    50.millis,
    1.second
  )

  def mkProducerSettings = ProducerSettings(
    Map(
      "bootstrap.servers" -> s"localhost:9092",
    ),
    5.seconds
  )

  implicit def decoder: KafkaDecoder[String] =
    KafkaDecoder.instance(rec => Right(new String(rec.value, "UTF-8")))

  implicit def encoder: KafkaEncoder[String] =
    KafkaEncoder.instance(str => (None, KafkaEncoder.Value(str.getBytes)))

  def produce(settings: ProducerSettings, topic: String, data: List[(Int, String)])(
    implicit ec: ExecutionContext) =
    Producer.create[IO](settings) use { producer =>
      implicit val cs = IO.contextShift(ec)
      data parTraverse {
        case (partition, msg) =>
          Producer.produce[IO, String](producer, msg, topic, partition, None)
      }
    }

  def nonEmptyStr = Gen.nonEmptyListOf(Gen.alphaLowerChar).map(_.mkString)

  implicit def noShrink[T]: Shrink[T] = Shrink.shrinkAny

  def createCustomTopic(topic: String,
                        topicConfig: Map[String, String] = Map.empty,
                        partitions: Int = 1,
                        replicationFactor: Int = 1): Resource[IO, Unit] =
    Resource
      .make(
        IO(
          AdminClient.create(Map[String, Object](
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:9092",
          ).asJava)))(client => IO(client.close))
      .flatMap { client =>
        Resource.make(IO {
          client
            .createTopics(Seq(new NewTopic(topic, partitions, replicationFactor.toShort)
              .configs(topicConfig.asJava)).asJava)
            .all
            .get(5, SECONDS)

          ()
        })(_ =>
          IO {
            client.deleteTopics(Seq(topic).asJava).all.get(5, SECONDS)
            ()
        })
      }
}
