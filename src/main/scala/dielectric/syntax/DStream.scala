package dielectric.syntax

import org.apache.spark.Partitioner
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration

import scala.reflect.ClassTag

import spire.algebra.{ Group, Monoid, Semigroup }

trait DStreamSyntax {
  implicit def dstreamSyntax[A](dstream: DStream[A]): DStreamOps[A] = new DStreamOps(dstream)

  implicit def dstreamPairSyntax[K, V](dstream: DStream[(K, V)]): DStreamPairOps[K, V] = new DStreamPairOps(dstream)
}

class DStreamOps[A](val dstream: DStream[A]) extends AnyVal {
  def groupReduceByWindow(windowDuration: Duration, slideDuration: Duration)(implicit A: Group[A]): DStream[A] =
    dstream.reduceByWindow(A.op, A.opInverse, windowDuration, slideDuration)
}

class DStreamPairOps[K, V](val dstream: DStream[(K, V)]) extends AnyVal {
  def groupReduceByKeyAndWindowWithPartitioner(windowDuration: Duration, slideDuration: Duration,
                                               partitioner: Partitioner, filterFunc: ((K, V)) => Boolean)(
                                               implicit K: ClassTag[K], V0: ClassTag[V], V1: Group[V]): DStream[(K, V)] =
    dstream.reduceByKeyAndWindow(V1.op _, V1.opInverse _, windowDuration, slideDuration, partitioner, filterFunc)

  def groupReduceByKeyAndWindowWithPartitions(windowDuration: Duration, slideDuration: Duration,
                                              numPartitions: Int, filterFunc: ((K, V)) => Boolean)(
                                              implicit K: ClassTag[K], V0: ClassTag[V], V1: Group[V]): DStream[(K, V)] =
    dstream.reduceByKeyAndWindow(V1.op _, V1.opInverse _, windowDuration, slideDuration, numPartitions, filterFunc)

  def withZero: DStreamPairWithMonoid[K, V] = DStreamPairWithMonoid(dstream)
}

class DStreamPairWithMonoid[K, V] private[dielectric](val dstream: DStream[(K, V)]) extends AnyVal {
  private def flattenBoth[W](dstream: DStream[(K, (Option[V], Option[W]))])(
                             implicit K: ClassTag[K], V: Monoid[V], W: Monoid[W]): DStreamPairWithMonoid[K, (V, W)] =
    DStreamPairWithMonoid(dstream.mapValues { case (l, r) => (l.getOrElse(V.id), r.getOrElse(W.id)) })

  private def flattenLeft[W](dstream: DStream[(K, (Option[V], W))])(
                             implicit K: ClassTag[K], V: Monoid[V]): DStreamPairWithMonoid[K, (V, W)] =
    DStreamPairWithMonoid(dstream.mapValues { case (l, r) => (l.getOrElse(V.id), r) })

  private def flattenRight[W](dstream: DStream[(K, (V, Option[W]))])(
                              implicit K: ClassTag[K], W: Monoid[W]): DStreamPairWithMonoid[K, (V, W)] =
    DStreamPairWithMonoid(dstream.mapValues { case (l, r) => (l, r.getOrElse(W.id)) })

  def fullOuterJoin[W](other: DStreamPairWithMonoid[K, W])(
                       implicit K: ClassTag[K], V0: ClassTag[V], V1: Monoid[V],
                       W0: ClassTag[W], W1: Monoid[W]): DStreamPairWithMonoid[K, (V, W)] =
    flattenBoth(dstream.fullOuterJoin(other.dstream))

  def fullOuterJoinWithPartitioner[W](other: DStreamPairWithMonoid[K, W], partitioner: Partitioner)(
                                      implicit K: ClassTag[K], V0: ClassTag[V], V1: Monoid[V],
                                      W0: ClassTag[W], W1: Monoid[W]): DStreamPairWithMonoid[K, (V, W)] =
    flattenBoth(dstream.fullOuterJoin(other.dstream, partitioner))

  def fullOuterJoinWithPartitions[W](other: DStreamPairWithMonoid[K, W], numPartitions: Int)(
                                     implicit K: ClassTag[K], V0: ClassTag[V], V1: Monoid[V],
                                     W0: ClassTag[W], W1: Monoid[W]): DStreamPairWithMonoid[K, (V, W)] =
    flattenBoth(dstream.fullOuterJoin(other.dstream, numPartitions))

  def leftOuterJoin[W](other: DStreamPairWithMonoid[K, W])(
                       implicit K: ClassTag[K], V: ClassTag[V],
                       W0: ClassTag[W], W1: Monoid[W]): DStreamPairWithMonoid[K, (V, W)] =
    flattenRight(dstream.leftOuterJoin(other.dstream))

  def leftOuterJoinWithPartitioner[W](other: DStreamPairWithMonoid[K, W], partitioner: Partitioner)(
                                      implicit K: ClassTag[K], V: ClassTag[V],
                                      W0: ClassTag[W], W1: Monoid[W]): DStreamPairWithMonoid[K, (V, W)] =
    flattenRight(dstream.leftOuterJoin(other.dstream, partitioner))

  def leftOuterJoinWithPartitions[W](other: DStreamPairWithMonoid[K, W], numPartitions: Int)(
                                     implicit K: ClassTag[K], V: ClassTag[V],
                                     W0: ClassTag[W], W1: Monoid[W]): DStreamPairWithMonoid[K, (V, W)] =
    flattenRight(dstream.leftOuterJoin(other.dstream, numPartitions))

  def rightOuterJoin[W](other: DStreamPairWithMonoid[K, W])(
                        implicit K: ClassTag[K], V0: ClassTag[V],
                        V1: Monoid[V], W: ClassTag[W]): DStreamPairWithMonoid[K, (V, W)] =
    flattenLeft(dstream.rightOuterJoin(other.dstream))

  def rightOuterJoinWithPartitioner[W](other: DStreamPairWithMonoid[K, W], partitioner: Partitioner)(
                                       implicit K: ClassTag[K], V0: ClassTag[V],
                                       V1: Monoid[V], W: ClassTag[W]): DStreamPairWithMonoid[K, (V, W)] =
    flattenLeft(dstream.rightOuterJoin(other.dstream, partitioner))

  def rightOuterJoinWithPartitions[W](other: DStreamPairWithMonoid[K, W], numPartitions: Int)(
                                      implicit K: ClassTag[K], V0: ClassTag[V],
                                      V1: Monoid[V], W: ClassTag[W]): DStreamPairWithMonoid[K, (V, W)] =
    flattenLeft(dstream.rightOuterJoin(other.dstream, numPartitions))
}

object DStreamPairWithMonoid {
  def apply[K, V](dstream: DStream[(K, V)]): DStreamPairWithMonoid[K, V] = new DStreamPairWithMonoid(dstream)
}
