package com.iravid.fs2.kafka.client

import cats.implicits._
import cats.effect.IO
import fs2.Stream
import org.scalacheck.Gen
import org.scalatest._
import org.scalatest.prop.GeneratorDrivenPropertyChecks

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class RecordStreamIntegrationSpec
    extends WordSpec with Matchers with KafkaSettings with GeneratorDrivenPropertyChecks {
  def recordStream(settings: ConsumerSettings, topic: String) =
    for {
      consumer     <- KafkaConsumer[IO](settings)
      recordStream <- RecordStream[IO, String](settings, consumer, Subscription.Topics(List(topic)))
    } yield recordStream

  def program(consumerSettings: ConsumerSettings,
              producerSettings: ProducerSettings,
              topic: String,
              data: List[String]) =
    for {
      _ <- produce(producerSettings, topic, data.tupleLeft(0))
      results <- recordStream(consumerSettings, topic) use { recordStream =>
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

  "The plain consumer" should {
    "work properly" in withRunningKafkaOnFoundPort(kafkaConfig) { config =>
      forAll((nonEmptyStr, "groupId"), (nonEmptyStr, "topic"), (Gen.listOf(Gen.alphaStr), "data")) {
        (groupId: String, topic: String, data: List[String]) =>
          val consumerSettings =
            mkConsumerSettings(config.kafkaPort, groupId, (data.length * 2) max 1)
          val producerSettings = mkProducerSettings(config.kafkaPort)
          val results =
            program(consumerSettings, producerSettings, topic, data)
              .unsafeRunSync()

          results.collect { case Right(a) => a } should contain theSameElementsAs data
      }
    }
  }
}
