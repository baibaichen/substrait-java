package org.apache.spark.sql

import com.google.protobuf.util.JsonFormat
import io.substrait.plan.PlanProtoConverter
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.gluten.GlutenWholeStage

object DebugUtils {

  def printSubstraitPlan(s: SparkPlan): String = {
    val stages = s.collect { case p: GlutenWholeStage => p }
    stages
      .map(_.doSubstraitGen())
      .map((new PlanProtoConverter).toProto)
      .map(JsonFormat.printer.print)
      .mkString("\n==================================================================\n")
  }

}
