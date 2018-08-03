package com.iravid.fs2.kafka.client

import cats.implicits._
import cats.effect.IO
import com.iravid.fs2.kafka.UnitSpec
import fs2.Stream
import org.scalacheck.Gen

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class RecordStreamIntegrationSpec extends UnitSpec with KafkaSettings {

  def partitionedProgram(consumerSettings: ConsumerSettings,
                         producerSettings: ProducerSettings,
                         topic: String,
                         data: List[(Int, String)]) = {
    val recordStream =
      for {
        consumer <- KafkaConsumer[IO](consumerSettings)
        recordStream <- RecordStream.partitioned[IO, String](
                         consumerSettings,
                         consumer,
                         Subscription.Topics(List(topic)),
                         None)
      } yield recordStream

    for {
      _ <- produce(producerSettings, topic, data)
      results <- recordStream use { stream =>
                  stream.records
                    .map {
                      case (_, stream) => stream
                    }
                    .joinUnbounded
                    .segmentN(data.length, true)
                    .map(_.force.toChunk)
                    .evalMap { recs =>
                      stream.commitQueue
                        .requestCommit(recs.foldMap(rec =>
                          CommitRequest(rec.env.topic, rec.env.partition, rec.env.offset)))
                        .as(recs.map(_.fa))
                    }
                    .flatMap(Stream.chunk(_).covary[IO])
                    .take(data.length.toLong)
                    .compile
                    .toVector
                    .timeout(30.seconds)
                }
    } yield results
  }

  def plainProgram(consumerSettings: ConsumerSettings,
                   producerSettings: ProducerSettings,
                   topic: String,
                   data: List[String]) = {
    val recordStream =
      for {
        consumer <- KafkaConsumer[IO](consumerSettings)
        recordStream <- RecordStream
                         .plain[IO, String](
                           consumerSettings,
                           consumer,
                           Subscription.Topics(List(topic)),
                           None)
      } yield recordStream

    for {
      _ <- produce(producerSettings, topic, data.tupleLeft(0))
      results <- recordStream use { recordStream =>
                  recordStream.records
                    .segmentN(data.length, true)
                    .map(_.force.toChunk)
                    .evalMap { records =>
                      val commitReq =
                        records.foldMap(record =>
                          CommitRequest(record.env.topic, record.env.partition, record.env.offset))
                      recordStream.commitQueue.requestCommit(commitReq).as(records.map(_.fa))
                    }
                    .flatMap(Stream.chunk(_).covary[IO])
                    .take(data.length.toLong)
                    .compile
                    .toVector
                    .timeout(30.seconds)
                }
    } yield results
  }

  "The plain consumer" should {
    "work properly" in withRunningKafkaOnFoundPort(kafkaConfig) { config =>
      forAll((nonEmptyStr, "groupId"), (nonEmptyStr, "topic"), (Gen.listOf(Gen.alphaStr), "data")) {
        (groupId: String, topic: String, data: List[String]) =>
          val consumerSettings =
            mkConsumerSettings(config.kafkaPort, groupId, 100)
          val producerSettings = mkProducerSettings(config.kafkaPort)
          val results =
            plainProgram(consumerSettings, producerSettings, topic, data)
              .unsafeRunSync()

          results.collect { case Right(a) => a } should contain theSameElementsAs data
      }
    }

    "handle data lengths bigger than the buffer size" in withRunningKafkaOnFoundPort(kafkaConfig) {
      config =>
        forAll((nonEmptyStr, "groupId"), (nonEmptyStr, "topic"), (Gen.listOf(Gen.alphaStr), "data")) {
          (groupId: String, topic: String, data: List[String]) =>
            val consumerSettings =
              mkConsumerSettings(config.kafkaPort, groupId, (data.length / 2) max 1)
            val producerSettings = mkProducerSettings(config.kafkaPort)
            val results =
              plainProgram(consumerSettings, producerSettings, topic, data)
                .unsafeRunSync()

            results.collect { case Right(a) => a } should contain theSameElementsAs data
        }
    }
  }

  "The partitioned consumer" should {
    "work properly" in withRunningKafkaOnFoundPort(kafkaConfig) { implicit config =>
      val dataGen = for {
        partitions <- Gen.chooseNum(1, 8)
        data       <- Gen.listOf(Gen.zip(Gen.chooseNum(0, partitions - 1), Gen.alphaStr))
      } yield (partitions, data)

      forAll((nonEmptyStr, "topic"), (nonEmptyStr, "groupId"), (dataGen, "data")) {
        case (topic, groupId, (partitions, data)) =>
          createCustomTopic(topic, partitions = partitions)

          val consumerSettings =
            mkConsumerSettings(config.kafkaPort, groupId, 100)
          val producerSettings = mkProducerSettings(config.kafkaPort)
          val results =
            partitionedProgram(consumerSettings, producerSettings, topic, data)
              .unsafeRunSync()

          results.collect { case Right(a) => a } should contain theSameElementsAs (data.map(_._2))
      }
    }
  }
}
