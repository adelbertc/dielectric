package dielectric.spark

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

import spire.algebra.Semigroup

trait RDDInstances {
  implicit def rddInstance[A : ClassTag]: Semigroup[RDD[A]] =
    new Semigroup[RDD[A]] {
      def op(x: RDD[A], y: RDD[A]): RDD[A] = x ++ y
    }
}
