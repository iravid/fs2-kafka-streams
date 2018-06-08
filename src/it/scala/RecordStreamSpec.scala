package com.iravid.fs2.kafka.client

import cats.implicits._
import cats.effect.IO
import com.iravid.fs2.kafka.codecs.{ KafkaDecoder, KafkaEncoder }
import org.scalatest._
import org.scalatest.Matchers._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random

trait KafkaSettings {
  def consumerSettings = ConsumerSettings(
    Map(
      "bootstrap.servers" -> "localhost:9092",
      "group.id"          -> "testbla",
      "auto.offset.reset" -> "earliest"
    ),
    10,
    10,
    10,
    50.millis,
    50.millis,
    10.seconds
  )

  def producerSettings = ProducerSettings(
    Map(
      "bootstrap.servers" -> "localhost:9092",
    ),
    5.seconds
  )

  implicit def decoder: KafkaDecoder[String] =
    KafkaDecoder.instance(rec => Right(new String(rec.value, "UTF-8")))

  implicit def encoder: KafkaEncoder[String] =
    KafkaEncoder.instance(str => (None, KafkaEncoder.Value(str.getBytes)))

  def testData = List("a", "b", "c", "d")

  def produce(data: List[String], topic: String, partition: Int = 0) =
    Producer.create[IO](producerSettings) use { producer =>
      data traverse { msg =>
        Producer.produce[IO, String](producer, msg, topic, partition, None)
      }
    }
}

class RecordStreamIntegrationSpec extends WordSpec with KafkaSettings {
  def recordStream(topic: String) =
    for {
      consumer <- KafkaConsumer[IO](consumerSettings)
      recordStream <- RecordStream[IO, String](
                       consumerSettings,
                       consumer,
                       Subscription.Topics(List(topic)))
    } yield recordStream

  def program(topic: String) =
    for {
      _ <- produce(testData, topic)
      results <- recordStream(topic) use { recordStream =>
                  recordStream.records
                    .evalMap { record =>
                      val commitReq =
                        CommitRequest(record.env.topic, record.env.partition, record.env.offset)
                      IO {
                        println(record.fa.getOrElse("Error"))
                        record.fa
                      } <* recordStream.commitQueue.requestCommit(commitReq)
                    }
                    .take(testData.length.toLong)
                    .compile
                    .toVector
                }
    } yield results

  "The plain consumer" should {
    "work properly" in {
      val results = program(Random.alphanumeric.take(5).mkString).unsafeRunSync()

      results.collect { case Right(a) => a } should contain theSameElementsAs testData
    }
  }
}

class PartitionedRecordStreamIntegrationSpec extends WordSpec with KafkaSettings {
  def partitionedRecordStream(topic: String) =
    for {
      consumer <- KafkaConsumer[IO](consumerSettings)
      recordStream <- PartitionedRecordStream[IO, String](
                       consumerSettings,
                       consumer,
                       Subscription.Topics(List(topic)))
    } yield recordStream

  def program(topic: String) =
    for {
      _ <- produce(testData, topic, 0)
      _ <- produce(testData, topic, 1)
      results <- partitionedRecordStream(topic) use { stream =>
                  stream.records
                    .evalMap {
                      case (tp, stream) =>
                        IO {
                          println(s"Assigned partition: ${tp}")
                          stream.onFinalize {
                            IO(println(s"Stream ended: ${tp}"))
                          }
                        }
                    }
                    .joinUnbounded
                    .evalMap { rec =>
                      IO {
                        println(rec.fa.getOrElse("Error"))
                        rec.fa
                      } <* stream.commitQueue.requestCommit(
                        CommitRequest(rec.env.topic, rec.env.partition, rec.env.offset))
                    }
                    .take(testData.length.toLong * 2)
                    .compile
                    .toVector
                    .timeout(10.seconds)
                }
    } yield results

  "The partitioned consumer" should {
    "work properly" in {
      val results = program(Random.alphanumeric.take(5).mkString)
        .unsafeRunSync()

      results.collect { case Right(a) => a } should contain theSameElementsAs (testData ++ testData)
    }
  }
}
