package com.iravid.fs2.kafka.streams

import cats.{ Functor, Monad }
import cats.effect.Sync
import cats.implicits._
import cats.effect.concurrent.Ref
import fs2.Stream
import scodec.{ Attempt, Codec, Err }

trait ReadOnlyTable[F[_], K, V] {
  def get(k: K): F[Option[V]]

  def getAll(ks: List[K]): F[Map[K, Option[V]]]

  def scan: Stream[F, (K, V)]
}

trait Table[F[_], K, V] { table =>
  def put(k: K, v: V): F[Unit]

  def putAll(data: List[(K, V)]): F[Unit]

  def get(k: K): F[Option[V]]

  def getAll(ks: List[K]): F[Map[K, Option[V]]]

  def commit(offset: Long): F[Unit]

  def lastCommittedOffset: F[Long]

  def delete(k: K): F[Unit]

  def scan: Stream[F, (K, V)]

  def view: ReadOnlyTable[F, K, V] =
    new ReadOnlyTable[F, K, V] {
      def get(k: K): F[Option[V]] = table.get(k)
      def getAll(ks: List[K]): F[Map[K, Option[V]]] = table.getAll(ks)
      def scan: Stream[F, (K, V)] = table.scan
    }
}

object InMemoryTable {
  case class State[K, V](data: Map[K, V], offset: Long)

  def create[F[_]: Functor: Sync, K, V]: F[InMemoryTable[F, K, V]] =
    Ref[F].of(State(Map.empty[K, V], 0L)).map(new InMemoryTable(_))
}

class InMemoryTable[F[_]: Functor, K, V](ref: Ref[F, InMemoryTable.State[K, V]])
    extends Table[F, K, V] {
  def put(k: K, v: V): F[Unit] =
    ref.update(state => state.copy(data = state.data + (k -> v)))

  def putAll(data: List[(K, V)]): F[Unit] =
    ref.update(state => state.copy(data = state.data ++ data))

  def get(k: K): F[Option[V]] =
    ref.get.map(_.data.get(k))

  def getAll(k: List[K]): F[Map[K, Option[V]]] =
    ref.get.map(state => k.fproduct(state.data.get).toMap)

  def commit(offset: Long): F[Unit] =
    ref.update(state => state.copy(offset = offset))

  def lastCommittedOffset: F[Long] =
    ref.get.map(_.offset)

  def delete(k: K): F[Unit] =
    ref.update(state => state.copy(data = state.data - k))

  def scan: Stream[F, (K, V)] =
    Stream.eval(ref.get).flatMap(state => Stream.emits(state.data.toList))
}

object RocksDBTable {
  case object CommitKey {
    import scodec.codecs._

    val value = "CommitKey"

    implicit val codec = utf8_32.exmap[CommitKey](
      {
        case `value` => Attempt.successful(CommitKey)
        case other   => Attempt.failure(Err.General(s"Bad value for CommitKey: ${other}", Nil))
      },
      _ => Attempt.successful(value)
    )

    implicit val K: Key.Aux[Codec, CommitKey.type, Long] =
      Key.instance(codec, ulong(63))
  }

  type CommitKey = CommitKey.type

  val DataCF = "data"
  val OffsetsCF = "offsets"

  def create[F[_]: Monad, K: Codec, V: Codec, CF](store: PolyKVStore.Aux[F, Codec, CF]) = {
    val dataCF = store.getColumnFamily(DataCF).flatMap {
      case Some(cf) => cf.pure[F]
      case None     => store.createColumnFamily(DataCF)
    }

    val offsetsCF = store.getColumnFamily(OffsetsCF).flatMap {
      case Some(cf) => cf.pure[F]
      case None     => store.createColumnFamily(OffsetsCF)
    }

    (dataCF, offsetsCF).mapN(new RocksDBTable[F, K, V, CF](store, _, _))
  }
}

class RocksDBTable[F[_]: Functor, K: Codec, V: Codec, CF](store: PolyKVStore.Aux[F, Codec, CF],
                                                          dataCF: CF,
                                                          offsetsCF: CF)
    extends Table[F, K, V] {
  import RocksDBTable._

  implicit val K: Key.Aux[Codec, K, V] = Key.instance

  def put(k: K, v: V): F[Unit] = store.put(dataCF, k, v)

  def putAll(data: List[(K, V)]): F[Unit] = store.putAll(dataCF, data)

  def get(k: K): F[Option[V]] = store.get(dataCF, k)

  def getAll(ks: List[K]): F[Map[K, Option[V]]] =
    store.getAll(dataCF, ks).map { retrievedMap =>
      ks.fproduct(retrievedMap.get).toMap
    }

  def delete(k: K): F[Unit] = store.delete(dataCF, k)

  def commit(offset: Long): F[Unit] =
    store.put(offsetsCF, CommitKey, offset)

  def lastCommittedOffset: F[Long] =
    store.get(offsetsCF, CommitKey).map(_.getOrElse(0L))

  def scan: Stream[F, (K, V)] =
    store.scan(dataCF)
}
