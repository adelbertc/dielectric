package dielectric.syntax

import org.apache.spark.Partitioner
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration

import scala.reflect.ClassTag

import spire.algebra.Group

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
}
