package org.apache.spark.substrait.expression

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum

import scala.reflect.ClassTag

case class Sig(expClass: Class[_], name: String)

class FunctionMappings {

  private def s[T <: Expression : ClassTag](name: String): Sig = Sig(scala.reflect.classTag[T].runtimeClass, name)

  val SCALAR_SIGS: Seq[Sig] = Seq(
    s[Add]("add"),
    s[Subtract]("subtract"),
    s[Multiply]("multiply"),
    s[Divide]("divide"),
    s[And]("and"),
    s[Or]("or"),
    s[Not]("not"),
    s[LessThan]("lt"),
    s[LessThanOrEqual]("lte"),
    s[GreaterThan]("gt"),
    s[GreaterThanOrEqual]("gte"),
    s[EqualTo]("equal"),
    // s[BitwiseXor]("xor"),
    s[IsNull]("is_null"),
    s[IsNotNull]("is_not_null")
  )

  val AGGREGATE_SIGS: Seq[Sig] = Seq(
    s[Sum]("sum")
  )

  lazy val scalar_functions_map: Map[Class[_], Sig] = SCALAR_SIGS.map(s => (s.expClass, s)).toMap
  lazy val aggregate_functions_map: Map[Class[_], Sig] = AGGREGATE_SIGS.map(s => (s.expClass, s)).toMap
}

object FunctionMappings extends FunctionMappings