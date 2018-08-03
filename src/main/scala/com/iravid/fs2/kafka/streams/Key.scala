package com.iravid.fs2.kafka.streams

trait Key[C[_], K] {
  type Value
  def KeyTC: C[K]
  def ValueTC: C[Value]
}

object Key {
  type Aux[C[_], K, V] = Key[C, K] { type Value = V }

  def instance[C[_], K: C, V: C]: Aux[C, K, V] =
    new Key[C, K] {
      type Value = V
      def KeyTC: C[K] = implicitly
      def ValueTC: C[V] = implicitly
    }
}
