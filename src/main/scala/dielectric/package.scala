import scalaz.Id.Id

package object dielectric {
  type SparkOp[S, A] = SparkOpT[Id, S, A]
}
