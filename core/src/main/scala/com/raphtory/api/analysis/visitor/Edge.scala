package com.raphtory.api.analysis.visitor

import PropertyMergeStrategy.PropertyMerge
import com.raphtory.utils.ExtendedNumeric.numericFromInt

import scala.reflect.ClassTag // implicit conversion from int to abstract numeric type

/** Extends [[EntityVisitor]] with edge-specific functionality
  *
  * The [[Edge]] class exposes the algorithm interface for accessing edge history and properties.
  *
  * @see
  * [[ExplodedEdge]]
  * [[PropertyMergeStrategy]]
  * [[EntityVisitor]]
  * [[Vertex]]
  */
trait Edge extends EntityVisitor {

  /** type of vertex IDs for this edge */
  type IDType

  //information about the edge meta data
  /** Edge ID */
  def ID: IDType

  /** ID of the source vertex of the edge */
  def src: IDType

  /** ID of the destination vertex of the edge */
  def dst: IDType

  /** `true` if the edge is an out-edge */
  def isOutgoing: Boolean = ID == dst

  /** `true` if the edge is an in-edge */
  def isIncoming: Boolean = ID == src

  /** Filter the edge from the `GraphPerspective`. */
  def remove(): Unit

  /** Compute the weight of the edge using a custom merge strategy
    *
    * @tparam A value type for the edge weight property (if `mergeStrategy` is not given, this needs to be a numeric type)
    * @tparam B return type of the merge strategy (only specify if using a custom merge strategy)
    *
    * @param weightProperty  edge property to use for computing edge weight
    * @param mergeStrategy merge strategy to use for converting property history to edge weight
    *                      (see [[PropertyMergeStrategy]] for predefined options or provide custom function). By default this returns the
    *                      sum of property values.
    * @param default default value for the weight property before applying the merge strategy.
    *                This defaults to `1` if `A` is a numeric type. This default value is applied before applying the
    *                merge strategy. In the case where, e.g., `mergeStrategy =  PropertyMergeStrategy.sum[A]`, the
    *                computed weight is the number of times the edge was active in the current view if the weight
    *                property is not found.
    */
  def weight[A, B](
      weightProperty: String = "weight",
      mergeStrategy: PropertyMerge[A, B],
      default: A
  ): B =
    getProperty(weightProperty, mergeStrategy) match {
      case Some(value) => value
      case None        => mergeStrategy(history().filter(_.event).map(h => PropertyValue(h.time, h.index, default)))
    }

  /** Compute the weight of the edge using a custom merge strategy
    *
    * @tparam A value type for the edge weight property (this needs to be a numeric type)
    * @tparam B return type of the merge strategy (only specify if using a custom merge strategy)
    *
    *  @param weightProperty  edge property to use for computing edge weight
    *  @param mergeStrategy merge strategy to use for converting property history to edge weight
    *                       (see [[PropertyMergeStrategy]]
    *                       for predefined options or provide custom function). By default this returns the
    *                       sum of property values.
    */
  def weight[A: Numeric, B](weightProperty: String, mergeStrategy: PropertyMerge[A, B]): B =
    weight(weightProperty, mergeStrategy, 1: A)

  /** Compute the weight of the edge using a custom merge strategy
    *
    * @tparam A value type for the edge weight property (if `mergeStrategy` is not given, this needs to be a numeric type)
    * @tparam B return type of the merge strategy (only specify if using a custom merge strategy)
    *
    *  @param mergeStrategy merge strategy to use for converting property history to edge weight
    *                       (see [[PropertyMergeStrategy]]
    *                       for predefined options or provide custom function). By default this returns the
    *                       sum of property values.
    */
  def weight[A: Numeric, B](mergeStrategy: PropertyMerge[A, B]): B =
    weight[A, B]("weight", mergeStrategy, 1: A)

  /** Compute the weight of the edge by sum
    *
    * @tparam A value type for the edge weight property - this needs to be a numeric type
    *
    *  @param weightProperty  edge property to use for computing edge weight
    *  @param default default value for the weight property before applying the merge strategy.
    *                 This defaults to `1`.
    */
  def weight[A: Numeric](weightProperty: String, default: A): A =
    weight(weightProperty, PropertyMergeStrategy.sum[A], default)

  /** Compute the weight of the edge by sum
    *
    * @tparam A value type for the edge weight property - this needs to be a numeric type
    *
    *  @param default default value for the weight property before applying the merge strategy. This defaults to `1`.
    */
  def weight[A: Numeric](default: A): A =
    weight("weight", default)

  /** Compute the weight of the edge by sum
    *
    * @tparam A value type for the edge weight property - this needs to be a numeric type
    * @param weightProperty  edge property to use for computing edge weight
    */
  def weight[A: Numeric](weightProperty: String): A =
    weight(weightProperty, 1: A)

  /** Compute the weight of the edge by sum
    *
    * @tparam A value type for the edge weight property - this needs to be a numeric type
    */
  def weight[A: Numeric](): A =
    weight("weight", 1: A)

  /** Send a message to the vertex connected on the other side of the edge
    * @param data Message data to send
    */
  def send(data: Any): Unit

  def setState(key: String, value: Any): Unit

  /** Retrieve value from algorithmic state
    * @tparam `T` value type for state
    * @param key key to use for retrieving state
    * @param includeProperties set this to `true` to fall-through to vertex properties if `key` is not found
    */
  def getState[T](key: String, includeProperties: Boolean = false): T

  /** Retrieve value from algorithmic state if it exists or return a default value otherwise
    * @tparam `T` value type for state
    * @param key key to use for retrieving state
    * @param value default value to return if state does not exist
    * @param includeProperties set this to `true` to fall-through to vertex properties
    *                          if `key` is not found in algorithmic state
    */
  def getStateOrElse[T](key: String, value: T, includeProperties: Boolean = false): T

  /** Checks if algorithmic state with key `key` exists
    * @param key state key to check
    * @param includeProperties Set this to `true` to fall-through to vertex properties if `key` is not found.
    *         If set, this function only returns `false` if `key` is not included in either algorithmic state
    *         or vertex properties
    */
  def containsState(key: String, includeProperties: Boolean = false): Boolean

  /** Retrieve value from algorithmic state if it exists or set this state to a default value and return otherwise
    * @tparam `T` value type for state
    * @param key key to use for retrieving state
    * @param value default value to set and return if state does not exist
    * @param includeProperties set this to `true` to fall-through to vertex properties
    *                          if `key` is not found in algorithmic state. State is only set if this is also not found.
    */
  def getOrSetState[T](key: String, value: T, includeProperties: Boolean = false): T

  /** Append new value to existing array or initialise new array if state does not exist
    * The value type of the state is assumed to be `Array[T]` if the state already exists.
    * @tparam `T` value type for state (needs to have a `ClassTag` available due to Scala `Array` implementation)
    * @param key key to use for retrieving state
    * @param value value to append to state
    */
  def appendToState[T: ClassTag](key: String, value: T): Unit

}
