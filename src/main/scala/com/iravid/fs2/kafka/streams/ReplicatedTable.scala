package com.iravid.fs2.kafka.streams

import cats.effect.{ Concurrent, Sync }
import cats.effect.concurrent.{ Deferred, Ref }
import cats.effect.implicits._
import cats.implicits._
import cats.effect.Resource
import fs2.Stream
import java.nio.charset.StandardCharsets
import org.rocksdb.RocksDB

trait ReadOnlyTable[F[_], K, V] {
  def get(k: K): F[Option[V]]
}

trait Table[F[_], K, V] extends ReadOnlyTable[F, K, V] {
  def set(k: K, v: V): F[Unit]

  def delete(k: K): F[Unit]
}

object Table {
  def inMemoryFromStream[F[_]: Concurrent, K, V](
    stream: Stream[F, (K, V)]): Resource[F, ReadOnlyTable[F, K, V]] = {
    val resources = for {
      ref      <- Ref[F].of(Map[K, V]())
      shutdown <- Deferred[F, Either[Throwable, Unit]]
      updateProcess <- stream
                        .interruptWhen(shutdown.get)
                        .evalMap(pair => ref.update(_ + pair))
                        .compile
                        .drain
                        .start
      table = new ReadOnlyTable[F, K, V] {
        def get(k: K): F[Option[V]] = ref.get.map(_.get(k))
      }
    } yield (shutdown, table)

    Resource
      .make(resources) {
        case (shutdown, _) =>
          shutdown.complete(Right(()))
      }
      .map(_._2)
  }

  def persistentFromStream[F[_]: Concurrent, K: ByteArrayCodec, V: ByteArrayCodec](
    path: String,
    stream: Stream[F, (K, V)]): Resource[F, ReadOnlyTable[F, K, V]] = {
    val resources = for {
      rocksdb <- Sync[F].delay {
                  RocksDB.open(path)
                }
      shutdown <- Deferred[F, Either[Throwable, Unit]]
      updateProcess <- stream
                        .interruptWhen(shutdown.get)
                        .evalMap {
                          case (key, value) =>
                            Sync[F].delay {
                              val serializedKey = ByteArrayCodec[K].encode(key)
                              val serializedValue = ByteArrayCodec[V].encode(value)

                              rocksdb.put(serializedKey, serializedValue)
                            }
                        }
                        .compile
                        .drain
                        .start
      table = new ReadOnlyTable[F, K, V] {
        def get(k: K): F[Option[V]] =
          Sync[F].delay {
            Option(rocksdb.get(ByteArrayCodec[K].encode(k))).traverse(ByteArrayCodec[V].decode(_))
          }.rethrow
      }
    } yield (rocksdb, shutdown, table)

    Resource
      .make(resources) {
        case (rocksdb, shutdown, _) =>
          shutdown.complete(Right(())) *>
            Sync[F].delay(rocksdb.close())
      }
      .map(_._3)
  }

}

trait ByteArrayCodec[T] {
  def encode(t: T): Array[Byte]
  def decode(bytes: Array[Byte]): Either[Throwable, T]
}

object ByteArrayCodec {
  def apply[T: ByteArrayCodec]: ByteArrayCodec[T] = implicitly

  implicit val stringCodec: ByteArrayCodec[String] =
    new ByteArrayCodec[String] {
      def encode(t: String): Array[Byte] = t.getBytes(StandardCharsets.UTF_8)
      def decode(bytes: Array[Byte]): Either[Throwable, String] =
        Either.catchNonFatal(new String(bytes, StandardCharsets.UTF_8))
    }
}
