package dielectric

import org.apache.spark.SparkContext

import scalaz.Id.Id

object SparkOp {
  def apply[S, A](f: (SparkContext, S) => (S, A)): SparkOp[S, A] =
    SparkOpT[Id, S, A](f)

  def reader[A](f: SparkContext => A): SparkOp[Unit, A] =
    SparkOpT.kleisli[Id, A](f)
}
