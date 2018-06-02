package com.iravid.fs2.kafka.client

import cats.effect.{ Async, Sync }
import cats.implicits._
import java.util.Properties
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
import org.apache.kafka.clients.consumer.{ ConsumerRebalanceListener, OffsetCommitCallback }

import scala.collection.JavaConverters._
import scala.concurrent.duration._

object KafkaConsumerFunctions {
  def createConsumer[F[_]: Sync](settings: Properties): F[ByteConsumer] =
    Sync[F].delay(new ByteConsumer(settings))

  def commit[F[_]: Async](consumer: ByteConsumer, data: OffsetMap): F[OffsetMap] =
    Async[F].async { cb =>
      consumer.commitAsync(
        data.asJava,
        new OffsetCommitCallback {
          override def onComplete(metadata: JOffsetMap, exception: Exception): Unit =
            if (exception eq null) cb(Right(metadata.asScala.toMap))
            else cb(Left(exception))
        }
      )
    }

  def poll[F[_]: Sync](consumer: ByteConsumer, timeout: FiniteDuration): F[List[ByteRecord]] =
    Sync[F].delay(consumer.poll(timeout.toMillis).iterator().asScala.toList)

  def subscribe[F[_]: Sync](consumer: ByteConsumer,
                            subscription: Subscription,
                            rebalanceListener: Option[ConsumerRebalanceListener]): F[Unit] = {
    val listener = rebalanceListener.getOrElse(new NoOpConsumerRebalanceListener)

    subscription match {
      case Subscription.Topics(topics) =>
        Sync[F].delay(consumer.subscribe(topics.asJava, listener))
      case Subscription.Pattern(pattern) =>
        for {
          pattern <- Sync[F].delay(java.util.regex.Pattern.compile(pattern))
          _       <- Sync[F].delay(consumer.subscribe(pattern, listener))
        } yield ()
    }
  }
}
