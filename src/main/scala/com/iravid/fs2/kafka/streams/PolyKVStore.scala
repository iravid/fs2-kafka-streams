package com.iravid.fs2.kafka.streams

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.implicits._
import fs2.Stream
import java.nio.charset.StandardCharsets
import org.rocksdb.{
  ColumnFamilyDescriptor,
  ColumnFamilyHandle => RocksDBColFHandle,
  RocksDB,
  WriteBatch,
  WriteOptions
}
import scodec.Codec
import scodec.bits.BitVector

import scala.collection.JavaConverters._

trait PolyKVStore[F[_], C[_]] { self =>
  type ColumnFamilyHandle

  def columnFamilies: F[List[ColumnFamilyHandle]]
  def createColumnFamily(name: String): F[ColumnFamilyHandle]
  def getColumnFamily(name: String): F[Option[ColumnFamilyHandle]]
  def dropColumnFamily(handle: ColumnFamilyHandle): F[Unit]

  def get[K](k: K)(implicit K: Key[C, K]): F[Option[K.Value]]
  def get[K](columnFamily: ColumnFamilyHandle, k: K)(implicit K: Key[C, K]): F[Option[K.Value]]

  def getAll[K](ks: List[K])(implicit K: Key[C, K]): F[Map[K, K.Value]]
  def getAll[K](columnFamily: ColumnFamilyHandle, ks: List[K])(
    implicit K: Key[C, K]): F[Map[K, K.Value]]

  def put[K, V](k: K, v: V)(implicit K: Key.Aux[C, K, V]): F[Unit]
  def put[K, V](columnFamily: ColumnFamilyHandle, k: K, v: V)(implicit K: Key.Aux[C, K, V]): F[Unit]

  def putAll[K, V](data: List[(K, V)])(implicit K: Key.Aux[C, K, V]): F[Unit]
  def putAll[K, V](columnFamily: ColumnFamilyHandle, data: List[(K, V)])(
    implicit K: Key.Aux[C, K, V]): F[Unit]

  def delete[K](k: K)(implicit K: Key[C, K]): F[Unit]
  def delete[K](columnFamily: ColumnFamilyHandle, k: K)(implicit K: Key[C, K]): F[Unit]

  def scan[K, V](implicit K: Key.Aux[C, K, V]): Stream[F, (K, V)]
  def scan[K, V](columnFamily: ColumnFamilyHandle)(implicit K: Key.Aux[C, K, V]): Stream[F, (K, V)]

  def monomorphize[K, V](implicit K: Key.Aux[C, K, V])
    : KVStore[F, K, V] { type ColumnFamilyHandle = self.ColumnFamilyHandle } =
    new KVStore[F, K, V] {
      type ColumnFamilyHandle = self.ColumnFamilyHandle

      def columnFamilies = self.columnFamilies
      def createColumnFamily(name: String) = self.createColumnFamily(name)
      def getColumnFamily(name: String) = self.getColumnFamily(name)
      def dropColumnFamily(handle: ColumnFamilyHandle) = self.dropColumnFamily(handle)

      def get(k: K) = self.get(k)
      def get(cf: ColumnFamilyHandle, k: K) = self.get(cf, k)

      def getAll(ks: List[K]) = self.getAll(ks)
      def getAll(cf: ColumnFamilyHandle, ks: List[K]) = self.getAll(cf, ks)

      def put(k: K, v: V) = self.put(k, v)
      def put(cf: ColumnFamilyHandle, k: K, v: V) = self.put(cf, k, v)

      def delete(k: K) = self.delete(k)
      def delete(cf: ColumnFamilyHandle, k: K) = self.delete(cf, k)

      def scan = self.scan
      def scan(cf: ColumnFamilyHandle) = self.scan(cf)
    }
}

object PolyKVStore {
  type Aux[F[_], C[_], CF] = PolyKVStore[F, C] { type ColumnFamilyHandle = CF }
}

class RocksDBPolyKVStore[F[_]](rocksdb: RocksDB,
                               rocksDbColumnFamilies: Ref[F, Map[Int, RocksDBColFHandle]],
                               defaultColumnFamily: RocksDBColFHandle)(implicit F: Sync[F])
    extends PolyKVStore[F, Codec] {
  type ColumnFamilyHandle = Int

  def columnFamilies: F[List[ColumnFamilyHandle]] =
    rocksDbColumnFamilies.get.map(_.values.map(_.getID).toList)
  def createColumnFamily(name: String) =
    F.delay(
        rocksdb.createColumnFamily(
          new ColumnFamilyDescriptor(name.getBytes(StandardCharsets.UTF_8))))
      .flatMap { handle =>
        rocksDbColumnFamilies.update(_ + (handle.getID -> handle)).as(handle.getID)
      }
  def getColumnFamily(name: String) =
    rocksDbColumnFamilies.get.map { cfMap =>
      cfMap
        .find {
          case (_, handle) =>
            new String(handle.getName, StandardCharsets.UTF_8) == name
        }
        .map(_._1)
    }
  def dropColumnFamily(handle: ColumnFamilyHandle): F[Unit] =
    rocksDbColumnFamilies.get
      .flatMap(_.get(handle).traverse_(h => F.delay(rocksdb.dropColumnFamily(h)))) *>
      rocksDbColumnFamilies.update(_ - handle)

  def get0[K](h: RocksDBColFHandle, k: K)(implicit K: Key[Codec, K]) =
    F.delay {
      Option(rocksdb.get(h, K.KeyTC.encode(k).require.toByteArray))
        .map(bytes => K.ValueTC.decodeValue(BitVector.view(bytes)).require)
    }

  def get[K](k: K)(implicit K: Key[Codec, K]): F[Option[K.Value]] = get0(defaultColumnFamily, k)
  def get[K](columnFamily: ColumnFamilyHandle, k: K)(
    implicit K: Key[Codec, K]): F[Option[K.Value]] =
    rocksDbColumnFamilies.get
      .flatMap(
        _.get(columnFamily)
          .flatTraverse(get0(_, k)))

  def getAll0[K](h: RocksDBColFHandle, ks: List[K])(implicit K: Key[Codec, K]): F[Map[K, K.Value]] =
    F.delay {
      val serializedKeys = ks.map(K.KeyTC.encode(_).require.toByteArray)
      val result =
        rocksdb.multiGet(List.fill(ks.size)(h).asJava, serializedKeys.asJava).asScala.toMap

      result.map {
        case (k, v) =>
          K.KeyTC.decodeValue(BitVector.view(k)).require ->
            K.ValueTC.decodeValue(BitVector.view(v)).require
      }
    }
  def getAll[K: Key[Codec, ?]](ks: List[K]) = getAll0(defaultColumnFamily, ks)
  def getAll[K](columnFamily: ColumnFamilyHandle, ks: List[K])(
    implicit K: Key[Codec, K]): F[Map[K, K.Value]] =
    rocksDbColumnFamilies.get.flatMap(_.get(columnFamily) match {
      case Some(h) => getAll0(h, ks)
      case None    => F.pure(Map())
    })

  def delete0[K](h: RocksDBColFHandle, k: K)(implicit K: Key[Codec, K]) = F.delay {
    rocksdb.delete(h, K.KeyTC.encode(k).require.toByteArray)
  }
  def delete[K: Key[Codec, ?]](k: K) = delete0(defaultColumnFamily, k)
  def delete[K: Key[Codec, ?]](columnFamily: ColumnFamilyHandle, k: K) =
    rocksDbColumnFamilies.get.flatMap(
      _.get(columnFamily)
        .traverse_(delete0(_, k)))

  def put0[K, V](h: RocksDBColFHandle, k: K, v: V)(implicit K: Key.Aux[Codec, K, V]) =
    F.delay {
      rocksdb.put(
        h,
        K.KeyTC.encode(k).require.toByteArray,
        K.ValueTC.encode(v).require.toByteArray
      )
    }
  def put[K, V](k: K, v: V)(implicit K: Key.Aux[Codec, K, V]) = put0(defaultColumnFamily, k, v)
  def put[K, V](columnFamily: ColumnFamilyHandle, k: K, v: V)(implicit K: Key.Aux[Codec, K, V]) =
    rocksDbColumnFamilies.get.flatMap(
      _.get(columnFamily)
        .traverse_(put0(_, k, v)))

  def putAll0[K, V](h: RocksDBColFHandle, data: List[(K, V)])(implicit K: Key.Aux[Codec, K, V]) =
    F.bracket(F.delay(new WriteBatch())) { writeBatch =>
      F.bracket(F.delay(new WriteOptions())) { writeOptions =>
        F.delay {
          data.foreach { kv =>
            writeBatch.put(
              h,
              K.KeyTC.encode(kv._1).require.toByteArray,
              K.ValueTC.encode(kv._2).require.toByteArray
            )
          }

          rocksdb.write(writeOptions, writeBatch)
        }
      }(wo => F.delay(wo.close()))
    }(wb => F.delay(wb.close()))

  def putAll[K, V](data: List[(K, V)])(implicit K: Key.Aux[Codec, K, V]): F[Unit] =
    putAll0(defaultColumnFamily, data)
  def putAll[K, V](columnFamily: ColumnFamilyHandle, data: List[(K, V)])(
    implicit K: Key.Aux[Codec, K, V]): F[Unit] =
    rocksDbColumnFamilies.get.flatMap(_.get(columnFamily).traverse_(putAll0(_, data)))

  def scan0[K, V](h: RocksDBColFHandle)(implicit K: Key.Aux[Codec, K, V]): Stream[F, (K, V)] =
    Stream
      .bracket {
        F.delay {
          val iterator = rocksdb.newIterator(h)
          iterator.seekToFirst()
          iterator
        }
      }(iterator => F.delay(iterator.close()))
      .flatMap { iterator =>
        Stream.repeatEval {
          F.delay {
            if (iterator.isValid()) {
              val key = K.KeyTC.decodeValue(BitVector.view(iterator.key())).require
              val value = K.ValueTC.decodeValue(BitVector.view(iterator.value())).require

              iterator.next()

              Some((key, value))
            } else None
          }
        }.unNoneTerminate
      }

  def scan[K, V](implicit K: Key.Aux[Codec, K, V]): Stream[F, (K, V)] = scan0(defaultColumnFamily)
  def scan[K, V](columnFamily: ColumnFamilyHandle)(
    implicit K: Key.Aux[Codec, K, V]): Stream[F, (K, V)] =
    Stream.eval(rocksDbColumnFamilies.get.map(_.get(columnFamily))) flatMap {
      case None    => Stream.empty
      case Some(h) => scan0(h)
    }
}
