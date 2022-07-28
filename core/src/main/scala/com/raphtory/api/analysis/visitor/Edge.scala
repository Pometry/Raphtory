package com.raphtory.api.analysis.visitor

import PropertyMergeStrategy.PropertyMerge
import com.raphtory.utils.ExtendedNumeric.numericFromInt // implicit conversion from int to abstract numeric type

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

}
