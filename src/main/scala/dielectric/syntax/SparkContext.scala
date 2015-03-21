package dielectric.syntax

import org.apache.spark.{ Accumulator, AccumulatorParam, SparkContext }

import spire.algebra.Monoid

trait SparkContextSyntax {
  implicit def sparkContextSyntax(sc: SparkContext): SparkContextOps = new SparkContextOps(sc)
}

class SparkContextOps(val sc: SparkContext) extends AnyVal {
  def monoidAccumulator[A : Monoid](start: A): Accumulator[A] = {
    val acc = new MonoidAccumulatorParam[A]
    sc.accumulator(start)(acc)
  }

  def monoidAccumulatorWithName[A : Monoid](start: A, name: String): Accumulator[A] = {
    val acc = new MonoidAccumulatorParam[A]
    sc.accumulator(start, name)(acc)
  }

  def zeroAccumulator[A](implicit A: Monoid[A]): Accumulator[A] =
    monoidAccumulator(A.id)

  def zeroAccumulatorWithName[A](name: String)(implicit A: Monoid[A]): Accumulator[A] =
    monoidAccumulatorWithName(A.id, name)
}

private class MonoidAccumulatorParam[A](implicit A: Monoid[A]) extends AccumulatorParam[A] {
  def addInPlace(r1: A, r2: A): A = A.op(r1, r2)

  def zero(initialValue: A): A = A.id
}
