package com.iravid.fs2.kafka.streams

import cats.effect.Sync
import cats.implicits._
import fs2.Stream
import org.rocksdb.RocksDB

trait Table[F[_], K, V] {
  def get(k: K): F[Option[V]]

  def set(k: K, v: V): F[Unit]

  def replicateTo(topic: String): F[ReplicatedTable[F, K, V]]
}

trait ReplicatedTable[F[_], K, V] extends Table[F, K, V] {
  def changelog: F[Stream[F, (K, V)]] // or maybe use the consumer here
}

trait ReadOnlyReplicatedTable[F[_], K, V] {
  def get(k: K): F[Option[V]]

  def detach: F[Table[F, K, V]]
}

object Table {
  def fromExistingTopic[F[_], K, V](topic: String): ReadOnlyReplicatedTable[F, K, V] = ???

  def local[F[_], K, V](path: String): Table[F, K, V] = ???

  // maybe add a builders class - local(path).replicated, local(path).unreplicated
}

trait ByteArrayCodec[T] {
  def encode(t: T): Array[Byte]
  def decode(bytes: Array[Byte]): Either[Throwable, T]
}

object ByteArrayCodec {
  def apply[T: ByteArrayCodec]: ByteArrayCodec[T] = implicitly
}

class RocksDBReplicatedTable[F[_]: Sync, K: ByteArrayCodec, V: ByteArrayCodec](db: RocksDB)
    extends ReplicatedTable[F, K, V] {
  def get(k: K): F[Option[V]] =
    Sync[F].delay {
      Option(db.get(ByteArrayCodec[K].encode(k))).traverse(ByteArrayCodec[V].decode(_))
    }.rethrow

  def set(k: K, v: V): F[Unit] = Sync[F].delay {
    db.put(ByteArrayCodec[K].encode(k), ByteArrayCodec[V].encode(v))
  }
}
