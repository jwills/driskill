package driskill
package scoobi

import core._
import com.nicta.scoobi.Scoobi._


case class ScoobiSDD[T](list: DList[T])(implicit wf: WireFormat[T], sc: ScoobiConfiguration = ScoobiConfiguration()) extends SDD[T] {
  /** use a neutral grouping by default */
  private implicit def grouping[T]: Grouping[T] = Grouping.groupingId[T]

  def flatMap(f: T => TraversableOnce[String]) = copy(list = list.mapFlatten(t => f(t).toIterable))

  def countByValue = ScoobiSDD[(T, Long)](list.map((_, 1L)).groupByKey.combine(Sum.long))

  def saveAsTextFile(path: String) = list.toTextFile(path).persist
}
