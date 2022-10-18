package org.apache.spark.sql.gluten

import io.substrait.relation.Aggregate
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GlutenHashAggregate(aggregate: BaseAggregateExec, child: GlutenPlan)
  extends SingleRel[Aggregate]{

  override protected def withNewChildInternal(newChild: SparkPlan): GlutenHashAggregate = {
    require(newChild.isInstanceOf[GlutenPlan])
    copy(child = newChild.asInstanceOf[GlutenPlan])
  }
  override def output: Seq[Attribute] = aggregate.output
  override def convert: Aggregate = null

}
