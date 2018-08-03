package com.iravid.fs2.kafka.streams

import cats.effect.concurrent.Ref
import cats.effect.{ Resource, Sync }
import cats.implicits._
import java.nio.file.Path
import org.rocksdb._
import scodec.Codec

import scala.collection.JavaConverters._

trait KVStores[C[_], P, F[_]] {
  def open(storeKey: P): Resource[F, PolyKVStore[F, C]]
}

class RocksDBKVStores[F[_]](implicit F: Sync[F]) extends KVStores[Codec, Path, F] {
  def listColumnFamilies(storeKey: Path): F[List[ColumnFamilyDescriptor]] =
    F.delay(
      RocksDB
        .listColumnFamilies(new Options(), storeKey.toAbsolutePath.toString)
        .asScala
        .toList
        .map(new ColumnFamilyDescriptor(_))
    )

  def open(storeKey: Path): Resource[F, PolyKVStore[F, Codec]] =
    Resource
      .make {
        for {
          cfDescs <- listColumnFamilies(storeKey)
          (store, handles) <- F.delay {
                               val handles = new java.util.ArrayList[ColumnFamilyHandle]()
                               val path = storeKey.toAbsolutePath.toString
                               val store =
                                 if (cfDescs.nonEmpty)
                                   RocksDB.open(path, cfDescs.asJava, handles)
                                 else
                                   RocksDB.open(
                                     new DBOptions()
                                       .setCreateIfMissing(true)
                                       .setCreateMissingColumnFamilies(true),
                                     path,
                                     List(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY)).asJava,
                                     handles
                                   )

                               (store, handles.asScala.toList)
                             }

          (default, rest) <- {
            val (before, after) =
              handles.span(h => !java.util.Arrays.equals(h.getName, RocksDB.DEFAULT_COLUMN_FAMILY))
            val default = after.headOption
            val rest = before ++ after.drop(1)

            default match {
              case Some(d) => F.pure((d, rest))
              case None =>
                F.raiseError[(ColumnFamilyHandle, List[ColumnFamilyHandle])](
                  new Exception("Could not locate default column family!"))
            }
          }

          handlesRef <- Ref[F].of(rest.map(h => h.getID -> h).toMap)
        } yield (store, handlesRef, default)
      } {
        case (rocksdb, columnFamilyHandles, defaultColumnFamily) =>
          for {
            handles <- columnFamilyHandles.get
            _ <- handles.values.toList.traverse_ { handle =>
                  F.delay(handle.close())
                }
            _ <- F.delay(defaultColumnFamily.close())
            _ <- F.delay(rocksdb.close())
          } yield ()
      }
      .map {
        case (rocksdb, columnFamilyHandles, defaultColumnFamily) =>
          new RocksDBPolyKVStore(
            rocksdb,
            columnFamilyHandles,
            defaultColumnFamily
          )
      }
}
