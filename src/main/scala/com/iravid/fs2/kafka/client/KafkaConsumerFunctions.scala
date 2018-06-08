package com.iravid.fs2.kafka.client

import cats.effect.{ Async, ConcurrentEffect, Sync, Timer }
import cats.implicits._
import com.iravid.fs2.kafka.EnvT
import com.iravid.fs2.kafka.codecs.KafkaDecoder
import com.iravid.fs2.kafka.model.{ ByteRecord, ConsumerMessage, Result }
import fs2.Pipe
import java.util.Properties
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
import org.apache.kafka.clients.consumer.{ ConsumerRebalanceListener, OffsetCommitCallback }
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.collection.JavaConverters._
import scala.concurrent.duration._

object KafkaConsumerFunctions {
  def createConsumer[F[_]: Sync](settings: Properties): F[ByteConsumer] =
    Sync[F].delay(new ByteConsumer(settings, new ByteArrayDeserializer, new ByteArrayDeserializer))

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

  def poll[F[_]](consumer: ByteConsumer,
                 pollTimeout: FiniteDuration,
                 wakeupTimeout: FiniteDuration)(implicit F: ConcurrentEffect[F],
                                                timer: Timer[F]): F[List[ByteRecord]] =
    F.race(
      timer.sleep(wakeupTimeout),
      F.cancelable[List[ByteRecord]] { cb =>
        val pollTask = F.delay(consumer.poll(pollTimeout.toMillis).iterator.asScala.toList)
        F.toIO(timer.shift *> pollTask).unsafeRunAsync(cb)

        F.toIO(F.delay(consumer.wakeup()))
      }
    ) flatMap {
      case Left(_)       => F.raiseError(new WakeupException)
      case Right(result) => F.pure(result)
    }

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

  def deserialize[F[_], T: KafkaDecoder]: Pipe[F, ByteRecord, ConsumerMessage[Result, T]] =
    _.map(rec => EnvT(rec, KafkaDecoder[T].decode(rec)))
}
