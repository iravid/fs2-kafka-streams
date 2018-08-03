package com.iravid.fs2.kafka.streams

import fs2.Stream

trait KVStore[F[_], K, V] {
  type ColumnFamilyHandle

  def columnFamilies: F[List[ColumnFamilyHandle]]
  def createColumnFamily(name: String): F[ColumnFamilyHandle]
  def getColumnFamily(name: String): F[Option[ColumnFamilyHandle]]
  def dropColumnFamily(handle: ColumnFamilyHandle): F[Unit]

  def get(k: K): F[Option[V]]
  def get(columnFamily: ColumnFamilyHandle, k: K): F[Option[V]]

  def getAll(ks: List[K]): F[Map[K, V]]
  def getAll(columnFamily: ColumnFamilyHandle, ks: List[K]): F[Map[K, V]]

  def put(k: K, v: V): F[Unit]
  def put(columnFamily: ColumnFamilyHandle, k: K, v: V): F[Unit]

  def delete(k: K): F[Unit]
  def delete(columnFamily: ColumnFamilyHandle, k: K): F[Unit]

  def scan: Stream[F, (K, V)]
  def scan(columnFamily: ColumnFamilyHandle): Stream[F, (K, V)]
}
