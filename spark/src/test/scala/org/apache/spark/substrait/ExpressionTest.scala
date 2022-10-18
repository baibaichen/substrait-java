package org.apache.spark.substrait

import scala.language.{existentials, implicitConversions}
import org.apache.spark.sql.catalyst.analysis.AnalysisTest
import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.catalyst.dsl.plans.DslLogicalPlan
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParserInterface}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation


class ExpressionTest extends AnalysisTest {

  val defaultParser = CatalystSqlParser
  val testRelation = LocalRelation($"a".int)

  def assertEqual(
      sqlCommand: String,
      e: Expression,
      parser: ParserInterface = defaultParser): Unit = {
    compareExpressions(parser.parseExpression(sqlCommand), e)
  }

  def assertConvert(e: Expression, output: Seq[Attribute] = Seq.empty): Unit = {
    val result = ExpressionConverter.defaultConverter.convert(e, output)
    assume(result.isDefined)
  }

  def parseExpression(sqlText: String): Expression = {
    defaultParser.parseExpression(sqlText)
  }

  test("add") {

    val y = Seq(
      Seq("req", "opt"),
      Seq("ts")
    )

    assertResult(Seq("req_ts","opt_ts"))(Util.crossProduct(y).map(list => list.mkString("_")))
    assertResult(Seq("i64_i64"))(Util.crossProduct(Seq(Seq("i64"), Seq("i64"))).map(list => list.mkString("_")))

    //assertConvert($"a" >= $"b" && $"a" <= $"c")
    //assertConvert(parseExpression("(1+2) as xxx"))
    assertConvert(parseExpression("2>1"))
  }

  ignore("sum") {
    val agg = Sum($"a")
    val query = testRelation
      .select(agg.toAggregateExpression().as("result"))
      .analyze
    val expression = query.expressions.head
    assertConvert(expression)
  }

  test("attribute") {
    val query = testRelation
      .select($"a")
      .analyze
    val expression = query.expressions.head
    assertConvert(expression, output = testRelation.output)
  }
}
