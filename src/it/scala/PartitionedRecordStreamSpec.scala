package com.iravid.fs2.kafka.client

import cats.effect.IO
import cats.implicits._
import fs2.Stream
import org.scalacheck.Gen
import org.scalatest.{ Matchers, WordSpec }
import org.scalatest.prop.GeneratorDrivenPropertyChecks

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class PartitionedRecordStreamIntegrationSpec
    extends WordSpec with Matchers with KafkaSettings with GeneratorDrivenPropertyChecks {
  def partitionedRecordStream(settings: ConsumerSettings, topic: String) =
    for {
      consumer <- KafkaConsumer[IO](settings)
      recordStream <- PartitionedRecordStream[IO, String](
                       settings,
                       consumer,
                       Subscription.Topics(List(topic)))
    } yield recordStream

  def program(consumerSettings: ConsumerSettings,
              producerSettings: ProducerSettings,
              topic: String,
              data: List[(Int, String)]) =
    for {
      _ <- produce(producerSettings, topic, data)
      results <- partitionedRecordStream(consumerSettings, topic) use { stream =>
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
            mkConsumerSettings(config.kafkaPort, groupId, (data.length * 2) max 1)
          val producerSettings = mkProducerSettings(config.kafkaPort)
          val results =
            program(consumerSettings, producerSettings, topic, data)
              .unsafeRunSync()

          results.collect { case Right(a) => a } should contain theSameElementsAs (data.map(_._2))
      }
    }
  }
}
