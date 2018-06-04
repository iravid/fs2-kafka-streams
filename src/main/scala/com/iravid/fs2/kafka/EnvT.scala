package com.iravid.fs2.kafka

import cats.{ Applicative, Eval, Traverse }

case class EnvT[E, F[_], A](env: E, fa: F[A])

object EnvT {
  implicit def traverse[E, F[_]: Traverse]: Traverse[EnvT[E, F, ?]] =
    new Traverse[EnvT[E, F, ?]] {
      def traverse[G[_], A, B](fa: EnvT[E, F, A])(f: A => G[B])(
        implicit G: Applicative[G]): G[EnvT[E, F, B]] =
        G.map(Traverse[F].traverse(fa.fa)(f))(EnvT(fa.env, _))

      def foldLeft[A, B](fa: EnvT[E, F, A], b: B)(f: (B, A) => B): B =
        Traverse[F].foldLeft(fa.fa, b)(f)

      def foldRight[A, B](fa: EnvT[E, F, A], b: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] =
        Traverse[F].foldRight(fa.fa, b)(f)

      override def map[A, B](fa: EnvT[E, F, A])(f: A => B): EnvT[E, F, B] =
        fa.copy(fa = Traverse[F].map(fa.fa)(f))
    }
}
