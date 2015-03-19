package dielectric

import org.apache.spark.SparkContext

import scalaz.{ Bind, Functor, IndexedStateT, Monad, MonadReader, MonadState }

/** SparkOpT - essentially an RWST minus the W. */
trait SparkOpT[F[_], S, A] { outer =>
  def run(sc: SparkContext, state: S): F[(S, A)]

  final def eval(sc: SparkContext, state: S)(implicit F: Functor[F]): F[A] =
    F.map(run(sc, state))(_._2)

  final def exec(sc: SparkContext, state: S)(implicit F: Functor[F]): F[S] =
    F.map(run(sc, state))(_._1)

  final def flatMap[B](f: A => SparkOpT[F, S, B])(implicit F: Bind[F]): SparkOpT[F, S, B] =
    SparkOpT((sc, state) => F.bind(run(sc, state)) { case (s, a) => f(a).run(sc, s) })

  final def indexedStateT(sc: SparkContext): IndexedStateT[F, S, S, A] =
    IndexedStateT(run(sc, _))

  final def map[B](f: A => B)(implicit F: Functor[F]): SparkOpT[F, S, B] =
    SparkOpT((sc, state) => F.map(run(sc, state)) { case (s, a) => (s, f(a)) })
}

object SparkOpT extends SparkOpTInstances {
  def apply[F[_], S, A](f: (SparkContext, S) => F[(S, A)]): SparkOpT[F, S, A] =
    new SparkOpT[F, S, A] {
      def run(sc: SparkContext, state: S): F[(S, A)] = f(sc, state)
    }
}

sealed abstract class SparkOpTInstances {
  implicit def sparkOpTMonad[F[_] : Monad, S]:
    MonadReader[({type l[a, b] = SparkOpT[F, S, b]})#l, SparkContext] with MonadState[SparkOpT[F, ?, ?], S] =
    new SparkOpTMonad[F, S] {}
}

private sealed abstract class SparkOpTMonad[F[_], S](implicit F: Monad[F])
    extends MonadReader[({type l[a, b] = SparkOpT[F, S, b]})#l, SparkContext]
    with    MonadState[SparkOpT[F, ?, ?], S] {

  def ask: SparkOpT[F, S, SparkContext] = SparkOpT((sc, s) => F.point((s, sc)))

  def bind[A, B](fa: SparkOpT[F, S, A])(f: A => SparkOpT[F, S, B]): SparkOpT[F, S, B] = fa.flatMap(f)

  def get: SparkOpT[F, S, S] = SparkOpT((_, s) => F.point((s, s)))

  def init: SparkOpT[F, S, S] = get

  def local[A](f: SparkContext => SparkContext)(fa: SparkOpT[F, S, A]): SparkOpT[F, S, A] =
    SparkOpT((sc, s) => fa.run(f(sc), s))

  def point[A](a: => A): SparkOpT[F, S, A] = SparkOpT((_, s) => F.point((s, a)))

  def put(s: S): SparkOpT[F, S, Unit] = SparkOpT((_, _) => F.point((s, ())))
}

