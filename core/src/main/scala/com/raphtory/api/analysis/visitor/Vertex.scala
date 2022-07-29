package com.raphtory.api.analysis.visitor

import com.raphtory.api.analysis.visitor
import EdgeDirection.Direction
import PropertyMergeStrategy.PropertyMerge

import scala.reflect.ClassTag

/** Extends [[EntityVisitor]] with vertex-specific functionality
  *
  * The `Vertex` is the main entry point for exploring the graph using a
  * [[com.raphtory.api.analysis.algorithm Algorithm]] given the node-centric nature of Raphtory.
  * It provides access to the edges of the graph and can send messages to and receive messages from other vertices.
  * A `Vertex` can also store computational state.
  */
trait Vertex extends EntityVisitor {

  /** ID type of this vertex */
  type IDType

  /** Concrete edge type for this vertex which implements [[com.raphtory.api.analysis.visitor.Edge Edge]] */
  type Edge <: visitor.ConcreteEdge[IDType]

  /** implicit ordering object for use when comparing vertex IDs */
  implicit val IDOrdering: Ordering[IDType]

  /** implicit ClassTag object for vertex IDType */
  implicit val IDClassTag: ClassTag[IDType] = ClassTag[IDType](ID.getClass)

  /** Get the ID type of this vertex */
  def ID: IDType

  /** Get the name of the vertex.
    * If `nameProperty` does not exist, this function returns the string representation of the vertex ID
    * @param nameProperty Vertex property to use as name (should uniquely identify the vertex)
    */
  def name(nameProperty: String = "name"): String =
    getPropertyOrElse[String](nameProperty, ID.toString)

  def name: String = name("name")

  //functionality for checking messages
  /** Check if vertex has received messages */
  def hasMessage: Boolean

  /** Queue of received messages
    * @tparam `T`  message data type
    */
  def messageQueue[T]: List[T]

  /** Vote to stop iterating (iteration stops if all vertices voted to halt) */
  def voteToHalt(): Unit

  /** Send data to this vertex at the next Step/Iteration
    * @param data message data to send
    */
  def messageSelf(data: Any): Unit = messageVertex(ID, data)

  /** Send data to another vertex at next Step/Iteration
    * @param vertexId Vertex Id of target vertex for the message
    * @param data message data to send
    */
  def messageVertex(vertexId: IDType, data: Any): Unit

  /** Send the same message data to all out-neighbours of this vertex
    * @param message message data to sent
    */
  def messageOutNeighbours(message: Any): Unit =
    outNeighbours.foreach(messageVertex(_, message))

  /** Send the same message data to all in- and out-neighbours of this vertex
    * @param message message data to sent
    */
  def messageAllNeighbours(message: Any): Unit =
    neighbours.foreach(messageVertex(_, message))

  /** Send the same message data to all in-neighbours of this vertex
    * @param message message data to sent
    */
  def messageInNeighbours(message: Any): Unit = inNeighbours.foreach(messageVertex(_, message))

  /** Get IDs of all out-neighbours of the vertex
    */
  def outNeighbours: List[IDType] =
    outEdges.map(_.dst)

  /** Get IDs fo all in-neighbours of the vertex
    * @param after only return neighbours that are active after time `after`
    * @param before only return neighbours that are active before time `before`
    */
  def inNeighbours: List[IDType] =
    inEdges.map(_.src)

  /** Get IDs of all in- and out-neighbours of the vertex
    */
  def neighbours: List[IDType] =
    (inNeighbours ++ outNeighbours).distinct

  /** Check if the vertex with ID `id` is an in- or out-neighbour of this vertex */
  def isNeighbour(id: IDType): Boolean =
    isInNeighbour(id) || isOutNeighbour(id)

  /** Check if the vertex with ID `id` is an in-neighbour of this vertex */
  def isInNeighbour(id: IDType): Boolean = inNeighbours.contains(id)

  /** Check if the vertex with ID `id` is an out-neighbour of this vertex */
  def isOutNeighbour(id: IDType): Boolean = outNeighbours.contains(id)

  //Degree
  /** Total number of neighbours (including in-neighbours and out-neighbours) of the vertex */
  def degree: Int = neighbours.size

  /** Number of out-neighbours of the vertex */
  def outDegree: Int = outNeighbours.size

  /** Number of in-neighbours of the vertex */
  def inDegree: Int = inNeighbours.size

  /** Return all edges starting or ending at this vertex
    */
  def edges: List[Edge] = inEdges ++ outEdges

  /** Return all edges starting at this vertex
    */
  def outEdges: List[Edge]

  /** Return all edges ending at this vertex
    */
  def inEdges: List[Edge]

  /** Return specified edge if it is an out-edge of this vertex
    * @param id ID of edge to return
    */
  def getOutEdge(
      id: IDType
  ): Option[Edge]

  /** Return specified edge if it is an in-edge of this vertex
    * @param id ID of edge to return
    */
  def getInEdge(
      id: IDType
  ): Option[Edge]

  /** Return specified edge if it is an in-edge or an out-edge of this vertex
    *
    * This function returns a list of edges, where the list is empty if neither an in-edge nor an out-edge
    * with this id exists, contains one element if either an in-edge or an out-edge with the id exists, or
    * contains two elements if both in-edge and out-edge exist.
    *
    * @param id ID of edge to return
    */
  def getEdge(
      id: IDType
  ): List[Edge]

  /** Set algorithmic state for this vertex
    * @param key key to use for setting value
    * @param value new value for state
    */
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

  // weight
  private def directedEdgeWeight[A, B: Numeric](
      dir: Direction = EdgeDirection.Incoming,
      weightProperty: String = "weight",
      edgeMergeStrategy: PropertyMerge[A, B],
      defaultWeight: A
  ): B =
    (dir match {
      case EdgeDirection.Incoming =>
        inEdges
      case EdgeDirection.Outgoing =>
        outEdges
      case EdgeDirection.Both     =>
        edges
    })
      .map(_.weight(weightProperty, edgeMergeStrategy, defaultWeight))
      .sum

  /** Sum of incoming edge weights.
    * For the meaning of the input arguments see [[com.raphtory.api.analysis.visitor.Edge Edge]]
    */
  def weightedInDegree[A, B: Numeric](
      weightProperty: String = "weight",
      edgeMergeStrategy: PropertyMerge[A, B],
      defaultWeight: A
  ): B =
    directedEdgeWeight(
            EdgeDirection.Incoming,
            weightProperty,
            edgeMergeStrategy,
            defaultWeight
    )

  /** Sum of incoming edge weights.
    * For the meaning of the input arguments see [[com.raphtory.api.analysis.visitor.Edge Edge]]
    */
  def weightedInDegree[A: Numeric, B: Numeric](
      weightProperty: String,
      edgeMergeStrategy: PropertyMerge[A, B]
  ): B =
    weightedInDegree(weightProperty, edgeMergeStrategy, DefaultValues.defaultVal[A])

  /** Sum of incoming edge weights.
    * For the meaning of the input arguments see [[com.raphtory.api.analysis.visitor.Edge Edge]]
    */
  def weightedInDegree[A: Numeric, B: Numeric](
      edgeMergeStrategy: PropertyMerge[A, B]
  ): B =
    weightedInDegree(DefaultValues.weightProperty, edgeMergeStrategy, DefaultValues.defaultVal[A])

  /** Sum of incoming edge weights.
    * For the meaning of the input arguments see [[com.raphtory.api.analysis.visitor.Edge Edge]]
    */
  def weightedInDegree[A: Numeric](
      weightProperty: String,
      defaultWeight: A
  ): A =
    weightedInDegree(weightProperty, DefaultValues.mergeStrategy[A], defaultWeight)

  /** Sum of incoming edge weights.
    * For the meaning of the input arguments see [[com.raphtory.api.analysis.visitor.Edge Edge]]
    */
  def weightedInDegree[A: Numeric](
      defaultWeight: A
  ): A =
    weightedInDegree(DefaultValues.weightProperty, DefaultValues.mergeStrategy[A], defaultWeight)

  /** Sum of incoming edge weights.
    * For the meaning of the input arguments see [[com.raphtory.api.analysis.visitor.Edge Edge]]
    */
  def weightedInDegree[A: Numeric](
      weightProperty: String
  ): A =
    weightedInDegree(weightProperty, DefaultValues.mergeStrategy[A], DefaultValues.defaultVal[A])

  /** Sum of incoming edge weights.
    * For the meaning of the input arguments see [[com.raphtory.api.analysis.visitor.Edge Edge]]
    */
  def weightedInDegree[A: Numeric](
  ): A =
    weightedInDegree(
            DefaultValues.weightProperty,
            DefaultValues.mergeStrategy[A],
            DefaultValues.defaultVal[A]
    )

  /** Sum of outgoing edge weights
    * For the meaning of the input arguments see [[com.raphtory.api.analysis.visitor.Edge Edge]]
    */
  def weightedOutDegree[A, B: Numeric](
      weightProperty: String = "weight",
      edgeMergeStrategy: PropertyMerge[A, B],
      defaultWeight: A
  ): B =
    directedEdgeWeight(
            EdgeDirection.Outgoing,
            weightProperty,
            edgeMergeStrategy,
            defaultWeight
    )

  /** Sum of outgoing edge weights
    * For the meaning of the input arguments see [[com.raphtory.api.analysis.visitor.Edge Edge]]
    */
  def weightedOutDegree[A: Numeric, B: Numeric](
      weightProperty: String,
      edgeMergeStrategy: PropertyMerge[A, B]
  ): B =
    weightedOutDegree(weightProperty, edgeMergeStrategy, DefaultValues.defaultVal[A])

  /** Sum of outgoing edge weights
    * For the meaning of the input arguments see [[com.raphtory.api.analysis.visitor.Edge Edge]]
    */
  def weightedOutDegree[A: Numeric, B: Numeric](
      edgeMergeStrategy: PropertyMerge[A, B]
  ): B =
    weightedOutDegree(DefaultValues.weightProperty, edgeMergeStrategy, DefaultValues.defaultVal[A])

  /** Sum of outgoing edge weights
    * For the meaning of the input arguments see [[com.raphtory.api.analysis.visitor.Edge Edge]]
    */
  def weightedOutDegree[A: Numeric](
      weightProperty: String,
      defaultWeight: A
  ): A =
    weightedOutDegree(weightProperty, DefaultValues.mergeStrategy[A], defaultWeight)

  /** Sum of outgoing edge weights
    * For the meaning of the input arguments see [[com.raphtory.api.analysis.visitor.Edge Edge]]
    */
  def weightedOutDegree[A: Numeric](
      defaultWeight: A
  ): A =
    weightedOutDegree(
            DefaultValues.weightProperty,
            DefaultValues.mergeStrategy[A],
            defaultWeight
    )

  /** Sum of outgoing edge weights
    * For the meaning of the input arguments see [[com.raphtory.api.analysis.visitor.Edge Edge]]
    */
  def weightedOutDegree[A: Numeric](
      weightProperty: String
  ): A =
    weightedOutDegree(weightProperty, DefaultValues.mergeStrategy[A], DefaultValues.defaultVal[A])

  /** Sum of outgoing edge weights
    * For the meaning of the input arguments see [[com.raphtory.api.analysis.visitor.Edge Edge]]
    */
  def weightedOutDegree[A: Numeric](
  ): A =
    weightedOutDegree(
            DefaultValues.weightProperty,
            DefaultValues.mergeStrategy[A],
            DefaultValues.defaultVal[A]
    )

  /** Sum of incoming and outgoing edge weights
    *  For the meaning of the input arguments see [[com.raphtory.api.analysis.visitor.Edge Edge]]
    */
  def weightedTotalDegree[A, B: Numeric](
      weightProperty: String = "weight",
      edgeMergeStrategy: PropertyMerge[A, B],
      defaultWeight: A
  ): B =
    directedEdgeWeight(
            EdgeDirection.Both,
            weightProperty,
            edgeMergeStrategy,
            defaultWeight
    )

  /** Sum of incoming and outgoing edge weights
    *  For the meaning of the input arguments see [[com.raphtory.api.analysis.visitor.Edge Edge]]
    */
  def weightedTotalDegree[A: Numeric, B: Numeric](
      weightProperty: String,
      edgeMergeStrategy: PropertyMerge[A, B]
  ): B =
    weightedTotalDegree(weightProperty, edgeMergeStrategy, DefaultValues.defaultVal[A])

  /** Sum of incoming and outgoing edge weights
    *  For the meaning of the input arguments see [[com.raphtory.api.analysis.visitor.Edge Edge]]
    */
  def weightedTotalDegree[A: Numeric, B: Numeric](
      edgeMergeStrategy: PropertyMerge[A, B]
  ): B =
    weightedTotalDegree[A, B](
            DefaultValues.weightProperty,
            edgeMergeStrategy,
            DefaultValues.defaultVal[A]
    )

  /** Sum of incoming and outgoing edge weights
    *  For the meaning of the input arguments see [[com.raphtory.api.analysis.visitor.Edge Edge]]
    */
  def weightedTotalDegree[A: Numeric](
      weightProperty: String,
      defaultWeight: A
  ): A =
    weightedTotalDegree(weightProperty, DefaultValues.mergeStrategy[A], defaultWeight)

  /** Sum of incoming and outgoing edge weights
    *  For the meaning of the input arguments see [[com.raphtory.api.analysis.visitor.Edge Edge]]
    */
  def weightedTotalDegree[A: Numeric](
      defaultWeight: A
  ): A =
    weightedTotalDegree(DefaultValues.weightProperty, DefaultValues.mergeStrategy[A], defaultWeight)

  /** Sum of incoming and outgoing edge weights
    *  For the meaning of the input arguments see [[com.raphtory.api.analysis.visitor.Edge Edge]]
    */
  def weightedTotalDegree[A: Numeric](
      weightProperty: String
  ): A =
    weightedTotalDegree(weightProperty, DefaultValues.mergeStrategy[A], DefaultValues.defaultVal[A])

  /** Sum of incoming and outgoing edge weights
    *  For the meaning of the input arguments see [[com.raphtory.api.analysis.visitor.Edge Edge]]
    */
  def weightedTotalDegree[A: Numeric](
  ): A =
    weightedTotalDegree(
            DefaultValues.weightProperty,
            DefaultValues.mergeStrategy[A],
            DefaultValues.defaultVal[A]
    )

  /** Filter this vertex and remove it and all its edges from the GraphPerspective */
  def remove(): Unit
}

private object DefaultValues {
  val weightProperty                                 = "weight"
  def mergeStrategy[T: Numeric]: PropertyMerge[T, T] = PropertyMergeStrategy.sum[T]
  def defaultVal[T](implicit numeric: Numeric[T]): T = numeric.one
}

private object EdgeDirection extends Enumeration {
  type Direction = Value
  val Incoming, Outgoing, Both = Value
}
