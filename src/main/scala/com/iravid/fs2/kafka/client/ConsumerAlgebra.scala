package com.iravid.fs2.kafka.client

import cats.effect.{ Resource, Sync }
import cats.implicits._
import cats.effect.{ ConcurrentEffect, Timer }
import com.iravid.fs2.kafka.model.ByteRecord
import java.util.{ Collection => JCollection, Properties }
import org.apache.kafka.clients.consumer.{ ConsumerRebalanceListener, ConsumerRecords }
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.concurrent.duration.FiniteDuration
import scala.collection.JavaConverters._

trait Consumer[F[_]] {
  def commit(data: OffsetMap): F[Unit]

  def poll(pollTimeout: FiniteDuration,
           wakeupTimeout: FiniteDuration): F[Map[TopicPartition, List[ByteRecord]]]

  def subscribe(subscription: Subscription, listener: Rebalance.Listener[F]): F[Unit]

  def unsubscribe: F[Unit]

  def pause(partitions: List[TopicPartition]): F[Unit]

  def resume(partitions: List[TopicPartition]): F[Unit]
}

class KafkaConsumer[F[_]](consumer: ByteConsumer)(implicit F: ConcurrentEffect[F], timer: Timer[F])
    extends Consumer[F] {
  def commit(data: OffsetMap): F[Unit] =
    F.delay(consumer.commitSync(data.asJava))

  def adaptConsumerRecords(
    records: ConsumerRecords[Array[Byte], Array[Byte]]): F[Map[TopicPartition, List[ByteRecord]]] =
    F.delay {
      val builder = Map.newBuilder[TopicPartition, List[ByteRecord]]
      val partitions = records.partitions().iterator()

      while (partitions.hasNext()) {
        val partition = partitions.next()
        val recordList = records.records(partition).iterator()
        val recordsBuilder = List.newBuilder[ByteRecord]

        while (recordList.hasNext()) {
          recordsBuilder += recordList.next()
        }

        builder += partition -> recordsBuilder.result()
      }

      builder.result()
    }

  def poll(pollTimeout: FiniteDuration,
           wakeupTimeout: FiniteDuration): F[Map[TopicPartition, List[ByteRecord]]] =
    F.race(
      timer.sleep(wakeupTimeout),
      F.cancelable[Map[TopicPartition, List[ByteRecord]]] { cb =>
        val pollTask = F
          .delay(consumer.poll(pollTimeout.toMillis))
          .flatMap(adaptConsumerRecords)
        F.toIO(timer.shift *> pollTask).unsafeRunAsync(cb)

        F.toIO(F.delay(consumer.wakeup()))
      }
    ) flatMap {
      case Left(_)       => F.raiseError(new WakeupException)
      case Right(result) => F.pure(result)
    }

  def subscribe(subscription: Subscription, listener: Rebalance.Listener[F]): F[Unit] = {
    val rebalanceListener = new ConsumerRebalanceListener {
      def onPartitionsAssigned(jpartitions: JCollection[TopicPartition]): Unit =
        F.toIO(listener(Rebalance.Assign(jpartitions.asScala.toList))).unsafeRunSync
      def onPartitionsRevoked(jpartitions: JCollection[TopicPartition]): Unit =
        F.toIO(listener(Rebalance.Revoke(jpartitions.asScala.toList))).unsafeRunSync
    }

    subscription match {
      case Subscription.Topics(topics) =>
        F.delay(consumer.subscribe(topics.asJava, rebalanceListener))
      case Subscription.Pattern(pattern) =>
        for {
          pattern <- F.delay(java.util.regex.Pattern.compile(pattern))
          _       <- F.delay(consumer.subscribe(pattern, rebalanceListener))
        } yield ()
    }
  }

  def unsubscribe: F[Unit] = F.delay(consumer.unsubscribe())

  def pause(partitions: List[TopicPartition]): F[Unit] =
    F.delay(consumer.pause(partitions.asJava))

  def resume(partitions: List[TopicPartition]): F[Unit] =
    F.delay(consumer.resume(partitions.asJava))
}

object KafkaConsumer {
  def consumer[F[_]](settings: Properties)(implicit F: Sync[F]) =
    Resource.make(
      F.delay(new ByteConsumer(settings, new ByteArrayDeserializer, new ByteArrayDeserializer))
    )(consumer => F.delay(consumer.close()))

  def apply[F[_]](settings: ConsumerSettings)(implicit F: ConcurrentEffect[F],
                                              timer: Timer[F]): Resource[F, Consumer[F]] =
    consumer(settings.driverProperties).map(new KafkaConsumer(_))
}
