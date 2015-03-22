package dielectric.syntax

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import scala.reflect.ClassTag

import scalaz.Applicative

import spire.algebra.{ Monoid, Semigroup }

trait RDDSyntax {
  implicit def rddSyntax[A](rdd: RDD[A]): RDDOps[A] = new RDDOps(rdd)

  implicit def rddPairSyntax[K, V](rdd: RDD[(K, V)]): RDDPairOps[K, V] = new RDDPairOps(rdd)
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

class RDDPairOps[K, V](val rdd: RDD[(K, V)]) extends AnyVal {
  import RDDPairOps._

  def appendByKey(implicit K: ClassTag[K], V0: ClassTag[V], V1: Semigroup[V]): RDD[(K, V)] =
    rdd.reduceByKey(V1.op)

  def appendByKeyLocally(implicit K: ClassTag[K], V0: ClassTag[V], V1: Semigroup[V]): Map[K, V] =
    rdd.reduceByKeyLocally(V1.op).toMap

  def appendByKeyWithPartitioner(partitioner: Partitioner)(
                                 implicit K: ClassTag[K], V0: ClassTag[V], V1: Semigroup[V]): RDD[(K, V)] =
    rdd.reduceByKey(partitioner, V1.op _)

  def appendByKeyWithPartitions(numPartitions: Int)(
                                implicit K: ClassTag[K], V0: ClassTag[V], V1: Semigroup[V]): RDD[(K, V)] =
    rdd.reduceByKey(V1.op _, numPartitions)

  def withZero: RDDPairWithDefault[K, V] = RDDPairWithDefault(rdd)
}

class RDDPairWithDefault[K, V] private[dielectric](val rdd: RDD[(K, V)]) extends AnyVal {
  private def flattenBoth[W](rdd: RDD[(K, (Option[V], Option[W]))])(
                             implicit K: ClassTag[K], V: Monoid[V], W: Monoid[W]): RDDPairWithDefault[K, (V, W)] =
    RDDPairWithDefault(rdd.mapValues { case (l, r) => (l.getOrElse(V.id), r.getOrElse(W.id)) })

  private def flattenLeft[W](rdd: RDD[(K, (Option[V], W))])(
                             implicit K: ClassTag[K], V: Monoid[V]): RDDPairWithDefault[K, (V, W)] =
    RDDPairWithDefault(rdd.mapValues { case (l, r) => (l.getOrElse(V.id), r) })

  private def flattenRight[W](rdd: RDD[(K, (V, Option[W]))])(
                              implicit K: ClassTag[K], W: Monoid[W]): RDDPairWithDefault[K, (V, W)] =
    RDDPairWithDefault(rdd.mapValues { case (l, r) => (l, r.getOrElse(W.id)) })

  def fullOuterJoin[W](other: RDDPairWithDefault[K, W])(
                       implicit K: ClassTag[K], V0: ClassTag[V],
                       V1: Monoid[V], W: Monoid[W]): RDDPairWithDefault[K, (V, W)] =
    flattenBoth(rdd.fullOuterJoin(other.rdd))

  def fullOuterJoinWithPartitioner[W](other: RDDPairWithDefault[K, W], partitioner: Partitioner)(
                                      implicit K: ClassTag[K], V0: ClassTag[V],
                                      V1: Monoid[V], W: Monoid[W]): RDDPairWithDefault[K, (V, W)] =
    flattenBoth(rdd.fullOuterJoin(other.rdd, partitioner))

  def fullOuterJoinWithPartitions[W](other: RDDPairWithDefault[K, W], numPartitions: Int)(
                                     implicit K: ClassTag[K], V0: ClassTag[V],
                                     V1: Monoid[V], W: Monoid[W]): RDDPairWithDefault[K, (V, W)] =
    flattenBoth(rdd.fullOuterJoin(other.rdd, numPartitions))

  def leftOuterJoin[W](other: RDDPairWithDefault[K, W])(
                       implicit K: ClassTag[K], V: ClassTag[V], W: Monoid[W]): RDDPairWithDefault[K, (V, W)] =
    flattenRight(rdd.leftOuterJoin(other.rdd))

  def leftOuterJoinWithPartitioner[W](other: RDDPairWithDefault[K, W], partitioner: Partitioner)(
                                      implicit K: ClassTag[K], V: ClassTag[V],W: Monoid[W]): RDDPairWithDefault[K, (V, W)] =
    flattenRight(rdd.leftOuterJoin(other.rdd, partitioner))

  def leftOuterJoinWithPartitions[W](other: RDDPairWithDefault[K, W], numPartitions: Int)(
                                     implicit K: ClassTag[K], V: ClassTag[V], W: Monoid[W]): RDDPairWithDefault[K, (V, W)] =
    flattenRight(rdd.leftOuterJoin(other.rdd, numPartitions))

  def rightOuterJoin[W](other: RDDPairWithDefault[K, W])(
                        implicit K: ClassTag[K], V0: ClassTag[V], V1: Monoid[V]): RDDPairWithDefault[K, (V, W)] =
    flattenLeft(rdd.rightOuterJoin(other.rdd))

  def rightOuterJoinWithPartitioner[W](other: RDDPairWithDefault[K, W], partitioner: Partitioner)(
                                       implicit K: ClassTag[K], V0: ClassTag[V], V1: Monoid[V]): RDDPairWithDefault[K, (V, W)] =
    flattenLeft(rdd.rightOuterJoin(other.rdd, partitioner))

  def rightOuterJoinWithPartitions[W](other: RDDPairWithDefault[K, W], numPartitions: Int)(
                                      implicit K: ClassTag[K], V0: ClassTag[V], V1: Monoid[V]): RDDPairWithDefault[K, (V, W)] =
    flattenLeft(rdd.rightOuterJoin(other.rdd, numPartitions))
}

object RDDPairWithDefault {
  def apply[K, V](rdd: RDD[(K, V)]): RDDPairWithDefault[K, V] = new RDDPairWithDefault(rdd)
}
