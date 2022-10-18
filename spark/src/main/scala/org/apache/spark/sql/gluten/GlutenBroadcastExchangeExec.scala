package org.apache.spark.sql.gluten

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.BroadcastExchangeLike
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util.concurrent.Future

/**
 * A [[GlutenBroadcastExchangeExec]] collects, transforms and finally broadcasts the result of
 * a transformed [[GlutenPlan]] SparkPlan in columnar format.
 */
case class GlutenBroadcastExchangeExec(mode: BroadcastMode, child: GlutenPlan)
  extends BroadcastExchangeLike
  with GlutenPlan {

  protected def withNewChildInternal(newChild: SparkPlan): GlutenBroadcastExchangeExec = {
    require(newChild.isInstanceOf[GlutenPlan])
    copy(child = newChild.asInstanceOf[GlutenPlan])
  }

  override def relationFuture: Future[Broadcast[Any]] =
    throw new UnsupportedOperationException()

  override protected def completionFuture: scala.concurrent.Future[Broadcast[Any]] = throw new UnsupportedOperationException()
  override def runtimeStatistics: Statistics = throw new UnsupportedOperationException()


  override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
    throw QueryExecutionErrors.executeCodePathUnsupportedError(nodeName)
}
