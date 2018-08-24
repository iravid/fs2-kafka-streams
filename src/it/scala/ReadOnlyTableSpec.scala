package com.iravid.fs2.kafka.streams

import cats.effect.Resource
import cats.implicits._
import cats.effect.IO
import com.iravid.fs2.kafka.UnitSpec
import com.iravid.fs2.kafka.client._
import com.iravid.fs2.kafka.codecs.{ KafkaDecoder, KafkaEncoder }
import fs2.Stream
import fs2.async.mutable.Signal
import java.nio.charset.StandardCharsets
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.scalacheck.Gen

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

case class Customer(userId: String, name: String)
object Customer {
  implicit val kafkaEncoder: KafkaEncoder[Customer] =
    KafkaEncoder.instance { customer =>
      val key = KafkaEncoder.Key(customer.userId.getBytes(StandardCharsets.UTF_8)).some
      val value =
        KafkaEncoder.Value(s"${customer.userId},${customer.name}".getBytes(StandardCharsets.UTF_8))

      (key, value)
    }

  implicit val kafkaDecoder: KafkaDecoder[Customer] =
    KafkaDecoder.instance { byteRecord =>
      val Array(userId, name) =
        new String(byteRecord.value, StandardCharsets.UTF_8).split(",")

      Right(Customer(userId, name))
    }
}

class ReadOnlyTableSpec extends UnitSpec with KafkaSettings {
  implicit val timer = IO.timer(global)
  implicit val shift = IO.contextShift(global)

  val userIdGen = Gen.oneOf("bob", "alice", "joe", "anyref")

  val customerGen = for {
    userId <- userIdGen
    name   <- Gen.identifier
  } yield Customer(userId, name)

  def customersProducer(producer: ByteProducer, interrupt: Signal[IO, Boolean]) =
    Stream
      .awakeEvery[IO](1.second)
      .evalMap(_ => IO(customerGen.sample.get))
      .evalTap(customer => IO(println(s"Customer: ${customer}")))
      .interruptWhen(interrupt)
      .evalMap(Producer.produce[IO, Customer](producer, _, "customers", 0, None))

  def customersTable(config: EmbeddedKafkaConfig) = {
    val consumerSettings = mkConsumerSettings(config.kafkaPort, "customers_consumer", 1000)

    for {
      consumer <- KafkaConsumer[IO](consumerSettings)
      recordStream <- RecordStream.plain[IO, Customer](
                       consumerSettings,
                       consumer,
                       Subscription.Topics(List("customers")),
                       None
                     )
      table <- Resource.liftF(Tables.inMemory.plain(recordStream)(_.userId))
    } yield table
  }

  def userClickStream(interrupt: Signal[IO, Boolean]) =
    Stream
      .awakeEvery[IO](1.second)
      .evalMap(_ => IO(userIdGen.sample.get))
      .interruptWhen(interrupt)

  def joinWith[A, K, V](stream: Stream[IO, A], table: ReadOnlyTable[IO, K, V])(key: A => K) =
    stream.evalMap(a => table.get(key(a)).tupleLeft(a))

  def program(config: EmbeddedKafkaConfig) =
    for {
      signal   <- Resource.liftF(Signal[IO, Boolean](false))
      producer <- Producer.create[IO](mkProducerSettings(config.kafkaPort))
      customersFiber <- Resource.liftF {
                         customersProducer(producer, signal).compile.drain.start
                       }
      table <- customersTable(config)
      printerFiber <- Resource.liftF(
                       joinWith(userClickStream(signal), table)(identity)
                         .evalTap(pair => IO(println(s"Join: ${pair}")))
                         .compile
                         .drain
                         .start
                     )
    } yield signal

  "A table-based program" must {
    "work properly" in withRunningKafkaOnFoundPort(kafkaConfig) { config =>
      val r = program(config) use { signal =>
        timer.sleep(10.seconds) >>
          signal.set(true)
      }

      r.unsafeRunSync()
    }
  }
}
