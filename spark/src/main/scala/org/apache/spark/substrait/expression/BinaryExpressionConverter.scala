package org.apache.spark.substrait.expression

import io.substrait.`type`.Type
import io.substrait.expression.{FunctionArg, Expression => PExp}
import io.substrait.function.SimpleExtension
import org.apache.spark.sql.catalyst.expressions.Expression

import scala.collection.JavaConverters

class BinaryExpressionConverter(functions: Seq[SimpleExtension.ScalarFunctionVariant])
  extends FunctionConverter[SimpleExtension.ScalarFunctionVariant, PExp](functions) {

  override def generateBinding(
      sparkExp: Expression,
      function: SimpleExtension.ScalarFunctionVariant,
      arguments: Seq[FunctionArg],
      outputType: Type): PExp = {
    PExp.ScalarFunctionInvocation.builder()
      .outputType(outputType)
      .declaration(function)
      .addAllArguments(JavaConverters.asJavaIterable(arguments))
      .build()
  }
  override def getSigs: Seq[Sig] = FunctionMappings.SCALAR_SIGS

  def convert(expression: Expression, operands: Seq[PExp]): Option[PExp] = {
    Option(signatures.get(expression.getClass))
      .filter(m => m.allowedArgCount(2))
      .flatMap(m => m.attemptMatch(expression, operands))
  }
}

object BinaryExpressionConverter {
  def apply(functions: Seq[SimpleExtension.ScalarFunctionVariant]): BinaryExpressionConverter = {
    new BinaryExpressionConverter(functions)
  }
}