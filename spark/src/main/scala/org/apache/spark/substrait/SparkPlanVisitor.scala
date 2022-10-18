package org.apache.spark.substrait

import org.apache.spark.sql.execution.SparkPlan

trait SparkPlanVisitor[T] {

  def visit(p: SparkPlan):T = p match {
    case _ => default(p)
  }

  def default(p: SparkPlan): T
}
