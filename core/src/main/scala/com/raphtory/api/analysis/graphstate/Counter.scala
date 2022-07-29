package com.raphtory.api.analysis.graphstate

import scala.collection.mutable

/**
  * Public interface for the Counter API within [[GraphState]].
  *
  *  A Counter maintains the distribution of categorical groupings such as community, connected component or node type.
  *  This enables measures such as the size/relative size of the largest group or largest k groups, filtering to include
  *  only the largest/largest k groups etc.
  *
  *  @tparam T Value type of the category being grouped (usually long if referring to a group or component id)
  *
  * @see
  * [[GraphState]], [[Histogram]]
  */

abstract class Counter[T] {

  /** Total of the counts */
  def totalCount : Int

  /** Getter function for the map of counts */
  def getCounts : mutable.Map[T,Int]

  /** Returns the id and count of the largest group */
  def largest : (T,Int)

  /** Returns the id and count of the k largest groups */
  def largest(k: Int): List[(T,Int)]

}
