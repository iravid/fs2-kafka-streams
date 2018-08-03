package com.iravid.fs2.kafka.client

import cats.{ Applicative, Apply, Functor, MonadError }
import cats.effect._, cats.effect.implicits._, cats.implicits._
import cats.effect.concurrent.{ Deferred, Ref }
import com.iravid.fs2.kafka.EnvT
import com.iravid.fs2.kafka.codecs.KafkaDecoder
import com.iravid.fs2.kafka.model.{ ByteRecord, ConsumerMessage, Result }
import fs2._
import fs2.async.mutable.Queue
import org.apache.kafka.common.TopicPartition

object RecordStream {
  case class Partitioned[F[_], T](
    commitQueue: CommitQueue[F],
    records: Stream[F, (TopicPartition, Stream[F, ConsumerMessage[Result, T]])])
  case class Plain[F[_], T](commitQueue: CommitQueue[F],
                            records: Stream[F, ConsumerMessage[Result, T]])

  case class InconsistentPartitionState(operation: String,
                                        partition: TopicPartition,
                                        tracker: Set[TopicPartition])
      extends Exception(s"""|Inconsistent partition state while executing ${operation}!
                            |Suspicious partition: ${partition}
                            |Partition tracker: ${tracker}""".stripMargin)

  case class PartitionHandle[F[_]](recordCount: Ref[F, Int],
                                   data: Queue[F, Option[Chunk[ByteRecord]]]) {
    def enqueue(chunk: Chunk[ByteRecord])(implicit F: Apply[F]): F[Unit] =
      recordCount.update(_ + chunk.size) *>
        data.enqueue1(chunk.some)

    def complete: F[Unit] = data.enqueue1(none)

    def dequeue(implicit F: Functor[F]): Stream[F, ByteRecord] =
      data.dequeue.unNoneTerminate
        .evalMap { chunk =>
          recordCount.update(_ - chunk.size).as(chunk)
        }
        .flatMap(Stream.chunk(_))
  }

  object PartitionHandle {
    def create[F[_]: Concurrent]: F[PartitionHandle[F]] =
      for {
        recordCount <- Ref[F].of(0)
        queue <- async
                  .unboundedQueue[F, Option[Chunk[ByteRecord]]]
      } yield PartitionHandle(recordCount, queue)
  }

  case class Resources[F[_], T](
    consumer: Consumer[F],
    polls: Stream[F, Poll.type],
    commits: CommitQueue[F],
    shutdownQueue: Queue[F, None.type],
    partitionTracker: Ref[F, Map[TopicPartition, PartitionHandle[F]]],
    pausedPartitions: Ref[F, Set[TopicPartition]],
    pendingRebalances: Ref[F, List[Rebalance]],
    partitionsQueue: Queue[
      F,
      Either[Throwable, Option[(TopicPartition, Stream[F, ConsumerMessage[Result, T]])]]]
  ) {
    def commandStream(implicit F: Concurrent[F])
      : Stream[F, Either[(Deferred[F, Either[Throwable, Unit]], CommitRequest), Poll.type]] =
      shutdownQueue.dequeue
        .mergeHaltL(commits.queue.dequeue.either(polls).map(_.some))
        .unNoneTerminate
  }

  def applyRebalanceEvents[F[_], T: KafkaDecoder](
    partitionTracker: Map[TopicPartition, PartitionHandle[F]],
    partitionsQueue: Queue[
      F,
      Either[Throwable, Option[(TopicPartition, Stream[F, ConsumerMessage[Result, T]])]]],
    rebalances: List[Rebalance])(
    implicit F: Concurrent[F]): F[Map[TopicPartition, PartitionHandle[F]]] =
    rebalances.foldLeftM(partitionTracker) {
      case (tracker, Rebalance.Assign(partitions)) =>
        partitions
          .traverse { tp =>
            partitionTracker.get(tp) match {
              case None =>
                for {
                  h <- PartitionHandle.create
                  _ <- partitionsQueue.enqueue1(
                        (tp, h.dequeue through deserialize[F, T]).some.asRight)
                } yield (tp, h)
              case Some(_) =>
                F.raiseError[(TopicPartition, PartitionHandle[F])](
                  InconsistentPartitionState("Rebalance.Assign", tp, tracker.keySet))
            }
          }
          .map(tracker ++ _)

      case (tracker, Rebalance.Revoke(partitions)) =>
        partitions
          .traverse_ { tp =>
            partitionTracker.get(tp) match {
              case Some(h) =>
                h.complete
              case None =>
                F.raiseError[Unit](
                  InconsistentPartitionState("Rebalance.Revoke", tp, tracker.keySet))
            }
          }
          .as(tracker -- partitions)
    }

  def resumePartitions[F[_]](
    settings: ConsumerSettings,
    pausedPartitions: Set[TopicPartition],
    partitionTracker: Map[TopicPartition, PartitionHandle[F]]
  )(implicit F: Concurrent[F]): F[List[TopicPartition]] =
    pausedPartitions.toList.flatTraverse { tp =>
      partitionTracker.get(tp) match {
        case Some(handle) =>
          handle.recordCount.get.map { count =>
            if (count <= settings.partitionOutputBufferSize)
              List(tp)
            else Nil
          }
        case None =>
          F.raiseError[List[TopicPartition]](
            InconsistentPartitionState("resumePartitions", tp, partitionTracker.keySet))
      }
    }

  def distributeRecords[F[_]](
    settings: ConsumerSettings,
    partitionTracker: Map[TopicPartition, PartitionHandle[F]],
    records: Map[TopicPartition, List[ByteRecord]])(implicit F: MonadError[F, Throwable]) =
    records.toList
      .flatTraverse {
        case (tp, records) =>
          partitionTracker.get(tp) match {
            case Some(handle) =>
              for {
                _           <- handle.enqueue(Chunk.seq(records))
                recordCount <- handle.recordCount.get
                shouldPause = if (recordCount <= settings.partitionOutputBufferSize)
                  Nil
                else
                  List(tp)
              } yield shouldPause
            case None =>
              F.raiseError[List[TopicPartition]](
                InconsistentPartitionState("distributeRecords", tp, partitionTracker.keySet))
          }
      }

  def commandHandler[F[_], T: KafkaDecoder](
    resources: Resources[F, T],
    settings: ConsumerSettings,
    command: Either[(Deferred[F, Either[Throwable, Unit]], CommitRequest), Poll.type])(
    implicit F: Concurrent[F]): F[Unit] =
    command match {
      case Left((deferred, req)) =>
        (resources.consumer
          .commit(req.offsets)
          .void
          .attempt >>= deferred.complete).void
      case Right(Poll) =>
        for {
          resumablePartitions <- for {
                                  paused    <- resources.pausedPartitions.get
                                  tracker   <- resources.partitionTracker.get
                                  resumable <- resumePartitions(settings, paused, tracker)
                                } yield resumable
          _ <- resources.consumer.resume(resumablePartitions) *>
                resources.pausedPartitions.update(_ -- resumablePartitions)

          records    <- resources.consumer.poll(settings.pollTimeout, settings.wakeupTimeout)
          rebalances <- resources.pendingRebalances.getAndSet(Nil).map(_.reverse)

          _ <- resources.partitionTracker.get >>=
                (applyRebalanceEvents(_, resources.partitionsQueue, rebalances)) >>=
                (resources.partitionTracker.set(_))

          partitionsToPause <- for {
                                tracker           <- resources.partitionTracker.get
                                partitionsToPause <- distributeRecords(settings, tracker, records)
                              } yield partitionsToPause
          _ <- resources.consumer.pause(partitionsToPause) *>
                resources.pausedPartitions.update(_ ++ partitionsToPause)
        } yield ()
    }

  def pollingLoop[F[_], T: KafkaDecoder](resources: Resources[F, T], settings: ConsumerSettings)(
    implicit F: Concurrent[F]) =
    resources.commandStream
      .evalMap(commandHandler(resources, settings, _))

  def recoverOffsets[F[_]](rebalance: Rebalance, recoveryFn: TopicPartition => F[Long])(
    implicit F: Applicative[F]): F[List[(TopicPartition, Long)]] =
    rebalance match {
      case Rebalance.Revoke(_) => F.pure(List())
      case Rebalance.Assign(tps) =>
        tps.traverse(tp => recoveryFn(tp).tupleLeft(tp))
    }

  def partitioned[F[_], T: KafkaDecoder](
    settings: ConsumerSettings,
    consumer: Consumer[F],
    subscription: Subscription,
    offsetRecoveryFn: Option[TopicPartition => F[Long]]
  )(implicit F: ConcurrentEffect[F], timer: Timer[F]): Resource[F, Partitioned[F, T]] =
    for {
      pendingRebalances <- Resource.liftF(Ref[F].of(List[Rebalance]()))
      rebalanceListener: Rebalance.Listener[F] = rebalance =>
        for {
          _ <- offsetRecoveryFn.traverse { fn =>
                for {
                  offsets <- recoverOffsets(rebalance, fn)
                  _       <- offsets.traverse_(tp => consumer.seek(tp._1, tp._2))
                } yield ()
              }
          _ <- pendingRebalances.update(rebalance :: _)
        } yield ()

      _ <- Resource.make(consumer.subscribe(subscription, rebalanceListener))(_ =>
            consumer.unsubscribe)

      resources <- Resource.liftF {
                    for {
                      partitionTracker <- Ref[F].of(Map.empty[TopicPartition, PartitionHandle[F]])
                      partitionsQueue <- async.unboundedQueue[
                                          F,
                                          Either[Throwable,
                                                 Option[(TopicPartition,
                                                         Stream[F, ConsumerMessage[Result, T]])]]]
                      pausedPartitions <- Ref[F].of(Set.empty[TopicPartition])
                      commitQueue      <- CommitQueue.create[F](settings.maxPendingCommits)
                      shutdownQueue    <- async.boundedQueue[F, None.type](1)
                      polls = Stream(Poll) ++ Stream.fixedRate(settings.pollInterval).as(Poll)

                    } yield
                      Resources(
                        consumer,
                        polls,
                        commitQueue,
                        shutdownQueue,
                        partitionTracker,
                        pausedPartitions,
                        pendingRebalances,
                        partitionsQueue)
                  }

      partitionsOut = resources.partitionsQueue.dequeue.rethrow.unNoneTerminate

      _ <- Resource.make {
            pollingLoop(resources, settings).compile.drain
              .handleErrorWith(e => resources.partitionsQueue.enqueue1(e.asLeft))
              .start
          }(fiber => resources.shutdownQueue.enqueue1(None) *> fiber.join)

    } yield Partitioned(resources.commits, partitionsOut)

  def plain[F[_]: ConcurrentEffect: Timer, T: KafkaDecoder](
    settings: ConsumerSettings,
    consumer: Consumer[F],
    subscription: Subscription,
    offsetRecoveryFn: Option[TopicPartition => F[Long]]): Resource[F, Plain[F, T]] =
    partitioned[F, T](settings, consumer, subscription, offsetRecoveryFn).map {
      case Partitioned(commitQueue, records) =>
        Plain(
          commitQueue,
          records.map {
            case (_, stream) => stream
          }.joinUnbounded
        )
    }

  def deserialize[F[_], T: KafkaDecoder]: Pipe[F, ByteRecord, ConsumerMessage[Result, T]] =
    _.map(rec => EnvT(rec, KafkaDecoder[T].decode(rec)))
}
