package dielectric.syntax

import dielectric.syntax.rdd.rddSyntax

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types._

import scala.reflect.ClassTag

import scalaz.\/

import shapeless.{ Generic, HList }
import shapeless.ops.traversable.FromTraversable
import shapeless.syntax.std.traversable._

trait DataFrameSyntax {
  implicit def dataFrameSyntax(dataFrame: DataFrame): DataFrameOps = new DataFrameOps(dataFrame)
}

final case class SchemaMismatch(required: List[StructField]) extends AnyVal {
  private def showDataType(sf: StructField): String = {
    val (prefix, suffix) = if (sf.nullable) ("Option[", "]") else ("", "")
    prefix ++ sf.dataType.typeName ++ suffix
  }

  override def toString: String = required.map(showDataType).mkString("(", ", ", ")")
}

class DataFrameOps(val dataFrame: DataFrame) extends AnyVal {
  /** Usage: someDataFrame.to[SomeCaseClass].rdd // SchemaMismatch \/ RDD[SomeCaseClass] */
  def to[P <: Product : ClassTag]: GenericDataFrameOps[P] = new GenericDataFrameOps(dataFrame)
}

class GenericDataFrameOps[P <: Product : ClassTag](val dataFrame: DataFrame) {
  private def rowToList(schema: List[StructField], r: Row): List[Any] =
    schema.zip(0.until(r.length)).map {
      case (s, i) => if (s.nullable) Option(r.get(i)) else r.get(i)
    }

  def rdd[L <: HList](implicit PL: Generic.Aux[P, L], L: FromTraversable[L]): \/[SchemaMismatch, RDD[P]] = {
    val schema = dataFrame.schema.fields.toList
    val untyped = dataFrame.rdd.map(rowToList(schema, _))

    untyped.traverse[\/[SchemaMismatch, ?], P] {
      _.toHList[L] match {
        case None => \/.left(SchemaMismatch(dataFrame.schema.fields.toList))
        case Some(l) => \/.right(PL.from(l))
      }
    }
  }
}
