package org.apache.spark.substrait

import org.apache.spark.sql.catalyst.expressions.Expression

@Deprecated
trait CallConverter[T] {
  def convert(expression:Expression, operands: Seq[T]) : Option[T]
}
