package com.iravid.fs2.kafka.streams

import cats.effect.{ Concurrent, Resource }
import cats.effect.implicits._
import cats.implicits._
import cats.kernel.Order
import com.iravid.fs2.kafka.EnvT
import com.iravid.fs2.kafka.client.{ CommitRequest, RecordStream }
import com.iravid.fs2.kafka.model.ByteRecord
import fs2.Stream
import java.nio.file.Path
import org.apache.kafka.common.TopicPartition
import scodec.Codec

object Tables {
  object inMemory {
    def partitioned[F[_]: Concurrent, K, T](recordStream: RecordStream.Partitioned[F, T])(
      key: T => K): Stream[F, (TopicPartition, ReadOnlyTable[F, K, T])] =
      recordStream.records.flatMap {
        case (tp, stream) =>
          Stream.eval(InMemoryTable.create[F, K, T]).flatMap { table =>
            val updateFiber = stream.chunks
              .evalMap { recordChunk =>
                val (offsets, data) = recordChunk.toList.collect {
                  case EnvT(metadata, Right(t)) =>
                    (metadata.offset, (key(t), t))
                }.unzip

                val commitOffset = offsets.maximumOption

                table.putAll(data) *>
                  commitOffset.traverse_ { offset =>
                    table.commit(offset) *>
                      recordStream.commitQueue.requestCommit(
                        CommitRequest(tp.topic, tp.partition, offset))
                  }
              }
              .compile
              .drain
              .start

            Stream.eval(updateFiber).as(tp -> table.view)
          }
      }

    def plain[F[_]: Concurrent, K, T](recordStream: RecordStream.Plain[F, T])(
      key: T => K): F[ReadOnlyTable[F, K, T]] =
      InMemoryTable.create[F, K, T].flatMap { table =>
        val updateFiber = recordStream.records.chunks
          .evalMap { recordChunk =>
            val (offsets, data) = recordChunk.toList.collect {
              case EnvT(metadata, Right(t)) =>
                (metadata, (key(t), t))
            }.unzip

            val commitOffset = offsets.maximumOption(Order.by((t: ByteRecord) => t.offset))

            table.putAll(data) *>
              commitOffset.traverse_ { metadata =>
                table.commit(metadata.offset) *>
                  recordStream.commitQueue.requestCommit(
                    CommitRequest(metadata.topic, metadata.partition, metadata.offset))
              }
          }
          .compile
          .drain
          .start

        updateFiber.as(table.view)
      }
  }

  object persistent {
    def partitioned[F[_]: Concurrent, K: Codec, V: Codec](
      stores: KVStores[Codec, Path, F],
      storeKey: TopicPartition => Path,
      recordStream: RecordStream.Partitioned[F, V])(
      key: V => K): Stream[F, (TopicPartition, ReadOnlyTable[F, K, V])] =
      recordStream.records.flatMap {
        case (tp, records) =>
          Stream.resource(stores.open(storeKey(tp))).flatMap { store =>
            Stream.eval(RocksDBTable.create[F, K, V, store.ColumnFamilyHandle](store)).flatMap {
              table =>
                val updateFiber = records.chunks
                  .evalMap { recordChunk =>
                    val (offsets, data) = recordChunk.toList.collect {
                      case EnvT(metadata, Right(t)) =>
                        (metadata.offset, (key(t), t))
                    }.unzip

                    val commitOffset = offsets.maximumOption

                    table.putAll(data) *>
                      commitOffset.traverse_ { offset =>
                        table.commit(offset) *>
                          recordStream.commitQueue.requestCommit(
                            CommitRequest(tp.topic, tp.partition, offset))
                      }

                  }
                  .compile
                  .drain
                  .start

                Stream.eval(updateFiber.as(tp -> table.view))
            }
          }
      }

    def plain[F[_]: Concurrent, K: Codec, V: Codec](
      stores: KVStores[Codec, Path, F],
      storeKey: Path,
      recordStream: RecordStream.Plain[F, V])(key: V => K): Resource[F, ReadOnlyTable[F, K, V]] =
      for {
        store <- stores.open(storeKey)
        table <- Resource.liftF(RocksDBTable.create[F, K, V, store.ColumnFamilyHandle](store))
        _ <- Resource.liftF {
              recordStream.records.chunks
                .evalMap { recordChunk =>
                  val (offsets, data) = recordChunk.toList.collect {
                    case EnvT(metadata, Right(t)) =>
                      (metadata, (key(t), t))
                  }.unzip

                  val commitOffset = offsets.maximumOption(Order.by((t: ByteRecord) => t.offset))

                  table.putAll(data) *>
                    commitOffset.traverse_ { metadata =>
                      table.commit(metadata.offset) *>
                        recordStream.commitQueue.requestCommit(
                          CommitRequest(metadata.topic, metadata.partition, metadata.offset))
                    }
                }
                .compile
                .drain
                .start
            }
      } yield table.view
  }
}
