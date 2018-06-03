package com.iravid.fs2.kafka.streams

import cats.effect.Sync
import cats.implicits._
import org.rocksdb.RocksDB

trait ReplicatedTable[F[_], K, V] {
  def get(k: K): F[Option[V]]

  def set(k: K, v: V): F[Unit]
}

object ReplicatedTable {
  // TODO: Should be Resource[F,...] and bracket the RocksDB open
  def apply[F[_], K, V]: F[ReplicatedTable[F, K, V]] = ???
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
