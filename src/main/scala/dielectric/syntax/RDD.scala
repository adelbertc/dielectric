package dielectric.syntax

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

import scalaz.Applicative

import spire.algebra.Monoid

trait RDDSyntax {
  implicit def rddSyntax[A](rdd: RDD[A]): RDDOps[A] = new RDDOps(rdd)
}

class RDDOps[A](val rdd: RDD[A]) extends AnyVal {
  def fold(implicit A0: Monoid[A], A1: ClassTag[A]): A = foldMap(identity)

  def foldMap[B : ClassTag](f: A => B)(implicit B: Monoid[B]): B = {
    def seqOp(u: B, t: A): B = B.op(u, f(t))

    def combOp(u1: B, u2: B): B = B.op(u1, u2)

    rdd.aggregate(B.id)(seqOp, combOp)
  }

  def sequence[G[_] : Applicative, B : ClassTag](implicit ev: A =:= G[B], GRDD: ClassTag[G[RDD[B]]]): G[RDD[B]] =
    traverse(ev.apply)

  def traverse[G[_], B : ClassTag](f: A => G[B])(implicit G: Applicative[G], GRDD: ClassTag[G[RDD[B]]]): G[RDD[B]] = {
    def seqOp(u: G[RDD[B]], t: A): G[RDD[B]] =
      G.apply2(u, G.map(f(t))(b => rdd.sparkContext.parallelize(List(b))))(_ ++ _)

    def combOp(u1: G[RDD[B]], u2: G[RDD[B]]): G[RDD[B]] =
      G.apply2(u1, u2)(_ ++ _)

    rdd.aggregate(G.point(rdd.sparkContext.emptyRDD[B]: RDD[B]))(seqOp, combOp)
  }
}
