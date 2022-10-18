package org.apache.spark.sql.gluten

import io.substrait.relation.Filter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{FilterExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.substrait.ExpressionConverter

case class GlutenFilterExec(
    filterExec: FilterExec,
    override val child: GlutenPlan)
  extends SingleRel[Filter] {

  override protected def withNewChildInternal(newChild: SparkPlan): GlutenFilterExec = {
    require(newChild.isInstanceOf[GlutenPlan])
    copy(child = newChild.asInstanceOf[GlutenPlan])
  }
  override def output: Seq[Attribute] = filterExec.output

  override def convert: Filter = {
    val exp = ExpressionConverter.defaultConverter(filterExec.condition, child.output)
    Filter.builder().condition(exp).input(substraitChild.convert).build()
  }
}
