package org.apache.spark.substrait

import scala.annotation.tailrec
import scala.collection.Seq
import scala.collection.mutable.ArrayBuffer

object Util {
  /**
   * Compute the cartesian product for n lists.
   *
   * <p>Based on <a
   * href="https://thomas.preissler.me/blog/2020/12/29/permutations-using-java-streams">Soln by
   * Thomas Preissler</a></a>
   */
  def crossProduct[T](lists: Seq[Seq[T]]): Seq[Seq[T]] = {

    /**
     *  list [a, b]
     *  element 1
     *  list  + element  ==>  [a, b, 1]
     */
    val appendElementToList: (Seq[T], T) => Seq[T] =
      (list, element) => list :+ element

    /**
     * ([a, b], [1, 2]) ==> [a, b, 1], [a, b, 2]
     */
    val appendAndGen: (Seq[T], Seq[T]) => Seq[Seq[T]] =
      (list, elemsToAppend) => elemsToAppend.map(e => appendElementToList(list, e))

    val firstListToJoin = lists.head
    val startProduct = appendAndGen(new ArrayBuffer[T], firstListToJoin)

    /** ([ [a, b], [c, d] ], [1, 2]) -> [a, b, 1], [a, b, 2], [c, d, 1], [c, d, 2] */
    val appendAndGenLists: (Seq[Seq[T]], Seq[T]) => Seq[Seq[T]] =
      (products, toJoin) => products.flatMap(product => appendAndGen(product, toJoin))

    lists.tail match {
      case Nil => appendAndGenLists(startProduct, Seq.empty)
      case l =>
        l.map(e => appendAndGenLists (startProduct, e))
        .reduce ((s1, s2) => Seq.concat (s1, s2) )
    }
  }

  def seqToOption[T](s: Seq[Option[T]]): Option[Seq[T]] = {
    @tailrec
    def seqToOptionHelper(s: Seq[Option[T]], accum: Seq[T] = Seq[T]()): Option[Seq[T]] = {
      s match {
        case Some(head) :: Nil => Option(head +: accum)
        case Some(head) :: tail => seqToOptionHelper(tail, head +: accum)
        case _ => None
      }
    }
    seqToOptionHelper(s)
  }


}
