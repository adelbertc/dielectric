package dielectric.spark

import org.apache.spark.streaming.dstream.DStream

import spire.algebra.Semigroup

trait DStreamInstances {
  implicit def dstreamInstance[A]: Semigroup[DStream[A]] =
    new Semigroup[DStream[A]] {
      def op(x: DStream[A], y: DStream[A]): DStream[A] = x union y
    }
}
