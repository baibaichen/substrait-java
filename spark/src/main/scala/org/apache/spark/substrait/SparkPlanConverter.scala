package org.apache.spark.substrait

import io.substrait.relation.{LocalFiles, Rel}
import org.apache.spark.sql.execution.FileSourceScanExec

class SparkPlanConverter {
  def scan(file: FileSourceScanExec): Option[Rel] = {
    TypeConverter
      .toNamedStruct(file.output)
      .map(LocalFiles.builder().initialSchema(_).build())
  }
}
