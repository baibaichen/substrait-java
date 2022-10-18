package org.apache.spark.substrait.expression

import io.substrait.expression.{Expression => PExp}
import io.substrait.expression.ExpressionCreator._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types._
import org.apache.spark.substrait.TypeConverter

class LiteralConverter extends Logging {

  object nonnull {
    val _bool: Boolean => PExp.Literal = bool(false, _)
    val _i8: Byte => PExp.Literal = i8(false, _)
    val _i16: Short => PExp.Literal = i16(false, _)
    val _i32: Int => PExp.Literal = i32(false, _)
    val _i64: Long => PExp.Literal = i64(false, _)
    val _fp32: Float => PExp.Literal = fp32(false, _)
    val _fp64: Double => PExp.Literal = fp64(false, _)
  }

  private def convertWithValue(literal: Literal): Option[PExp.Literal] = {
    Option.apply(
      literal match {
        case Literal(b: Boolean, BooleanType) => nonnull._bool(b)
        case Literal(b: Byte, ByteType) => nonnull._i8(b)
        case Literal(s: Short, ShortType) => nonnull._i16(s)
        case Literal(i: Integer, IntegerType) => nonnull._i32(i)
        case Literal(l: Long, LongType) => nonnull._i64(l)
        case Literal(f: Float, FloatType) => nonnull._fp32(f)
        case Literal(d: Double, DoubleType) => nonnull._fp64(d)
        case _ => null
      }
    )
  }

  def convert(literal: Literal): Option[PExp.Literal] = {
    if (literal.nullable) {
      TypeConverter.convert(literal.dataType, nullable = true)
        .map(typedNull)
    } else {
      convertWithValue(literal)
    }
  }
}

object LiteralConverter extends LiteralConverter