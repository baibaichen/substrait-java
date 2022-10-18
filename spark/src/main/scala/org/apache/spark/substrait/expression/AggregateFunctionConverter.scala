package org.apache.spark.substrait.expression

import io.substrait.`type`.Type
import io.substrait.expression.{AggregateFunctionInvocation, ExpressionCreator, FunctionArg, Expression => PExp}
import io.substrait.function.SimpleExtension
import io.substrait.proto.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateMode, Final, Partial, PartialMerge}

import java.util.Collections
import scala.collection.JavaConverters

class AggregateFunctionConverter (functions: Seq[SimpleExtension.AggregateFunctionVariant])
  extends FunctionConverter[SimpleExtension.AggregateFunctionVariant, AggregateFunctionInvocation](functions) {

  override def generateBinding(
      sparkExp: Expression,
      function: SimpleExtension.AggregateFunctionVariant,
      arguments: Seq[FunctionArg],
      outputType: Type): AggregateFunctionInvocation = {

    val sparkAggregate = sparkExp.asInstanceOf[AggregateExpression]

    val invocation = if (sparkAggregate.isDistinct) {
      AggregateFunction.AggregationInvocation.AGGREGATION_INVOCATION_DISTINCT
    } else {
      AggregateFunction.AggregationInvocation.AGGREGATION_INVOCATION_ALL
    }
    ExpressionCreator.aggregateFunction(
      function,
      outputType,
      to(sparkAggregate.mode),
      Collections.emptyList[PExp.SortField](),
      invocation,
      JavaConverters.asJavaIterable(arguments))
  }

  private def to(mode: AggregateMode) : PExp.AggregationPhase =  mode match {
    case Partial => PExp.AggregationPhase.INITIAL_TO_INTERMEDIATE
    case PartialMerge => PExp.AggregationPhase.INTERMEDIATE_TO_INTERMEDIATE
    case Final => PExp.AggregationPhase.INTERMEDIATE_TO_RESULT
    case other => throw new UnsupportedOperationException(s"not currently supported: $other.")
  }
  override def getSigs: Seq[Sig] = FunctionMappings.AGGREGATE_SIGS

  def convert(expression: AggregateExpression, operands: Seq[PExp]): Option[AggregateFunctionInvocation] = {
    Option(signatures.get(expression.aggregateFunction.getClass))
      .filter(m => m.allowedArgCount(2))
      .flatMap(m => m.attemptMatch(expression, operands))
  }
}

object AggregateFunctionConverter {
  def apply(functions: Seq[SimpleExtension.AggregateFunctionVariant]): AggregateFunctionConverter = {
    new AggregateFunctionConverter(functions)
  }
}