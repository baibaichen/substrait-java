package org.apache.spark.sql.gluten

import io.substrait.relation.LocalFiles
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{FileSourceScanExec, LeafExecNode}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GlutenFileScan(fileScan: FileSourceScanExec)
  extends LeafExecNode
    with GlutenPlan
    with SubstraitSupport[LocalFiles] {

  override def output: Seq[Attribute] = fileScan.output

  override def convert: LocalFiles = {
    Substrait.localFiles(output)(
      throw new UnsupportedOperationException(s"$nodeName.convert() fails")
    )
  }

  override def inputColumnarRDDs: Seq[RDD[ColumnarBatch]] = Nil
}


