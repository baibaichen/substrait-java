package org.apache.spark.sql.gluten

import io.substrait.relation.{Join, LocalFiles, Rel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashJoin, ShuffledHashJoinExec}
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.substrait.ExpressionConverter

trait GlutenJoinExec[T <: HashJoin]
  extends BinaryExecNode
    with GlutenPlan
    with SubstraitSupport[Join] {
    def joinExec: T
    def left: GlutenPlan
    def right: GlutenPlan

  override def output: Seq[Attribute] = joinExec.output

  override def convert: Join = {
    val left_ = leftRel
    val right_ = rightRel

    // TODO: handle cross join
    val joinType = joinExec.joinType match {
      case Inner => Join.JoinType.INNER
      case LeftOuter => Join.JoinType.LEFT
      case RightOuter => Join.JoinType.RIGHT
      case FullOuter => Join.JoinType.OUTER
      case LeftSemi => Join.JoinType.SEMI
      case LeftAnti => Join.JoinType.ANTI
    }

    val builder = Join.builder()
    joinExec.condition
      .map(e => ExpressionConverter.defaultConverter(e))
      .foreach(builder.condition)

    builder
      .joinType(joinType)
      .left(left_)
      .right(right_)
      .build()
  }

  protected lazy val (buildPlan, streamedPlan) = joinExec.buildSide match {
    case BuildLeft => (left, right)
    case BuildRight => (right, left)
  }

  protected def leftRel: Rel

  protected def rightRel: Rel
}

case class GlutenBroadcastHashJoinExec(
    override val joinExec: BroadcastHashJoinExec,
    override val left: GlutenPlan,
    override val right: GlutenPlan)
  extends GlutenJoinExec[BroadcastHashJoinExec] {

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): GlutenBroadcastHashJoinExec = {
    copy(left = newLeft.asInstanceOf[GlutenPlan], right = newRight.asInstanceOf[GlutenPlan])
  }
  override def inputColumnarRDDs: Seq[RDD[ColumnarBatch]] =
    streamedPlan.asInstanceOf[SubstraitSupport[_]].inputColumnarRDDs

  override protected lazy val leftRel: Rel = joinExec.buildSide match {
    case BuildLeft =>
      Substrait.localFiles(left.output)(
        throw new UnsupportedOperationException(s"${left.nodeName}.convert() fails")
      )
    case BuildRight => left.asInstanceOf[SubstraitSupport[Rel]].convert
  }

  override protected lazy val rightRel: Rel = joinExec.buildSide match {
    case BuildLeft => right.asInstanceOf[SubstraitSupport[Rel]].convert
    case BuildRight => Substrait.localFiles(right.output)(
      throw new UnsupportedOperationException(s"${right.nodeName}.convert() fails")
    )
  }
}

case class GlutenShuffledHashJoinExec(
    override val joinExec: ShuffledHashJoinExec,
    override val left: GlutenPlan,
    override val right: GlutenPlan)
  extends GlutenJoinExec[ShuffledHashJoinExec] {

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): GlutenShuffledHashJoinExec = {
    copy(left = newLeft.asInstanceOf[GlutenPlan], right = newRight.asInstanceOf[GlutenPlan])
  }
  override def inputColumnarRDDs: Seq[RDD[ColumnarBatch]] =
    streamedPlan.asInstanceOf[SubstraitSupport[_]].inputColumnarRDDs.head ::
      buildPlan.asInstanceOf[SubstraitSupport[_]].inputColumnarRDDs.head :: Nil

  override protected lazy val leftRel: Rel = left.asInstanceOf[SubstraitSupport[Rel]].convert

  override protected lazy val rightRel: Rel = right.asInstanceOf[SubstraitSupport[Rel]].convert
}