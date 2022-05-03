package com.raphtory.graph.visitor

import PropertyMergeStrategy.PropertyMerge
import com.raphtory.graph.visitor.EdgeDirection.Direction
import com.raphtory.graph.visitor

import scala.reflect.ClassTag

/**
  * `Vertex`
  *   : Extends [`EntityVisitor`](com.raphtory.graph.visitor.EntityVisitor) with vertex-specific functionality
  *
  * The `Vertex` is the main entry point for exploring the graph using a
  * [`GraphAlgorithm`](com.raphtory.algorithms.api.GraphAlgorithm) given the node-centric nature of Raphtory.
  * It provides access to the edges of the graph and can send messages to and receive messages from other vertices.
  * A `Vertex` can also store computational state.
  *
  * ## Generic types
  *
  * `IDType`
  *   : ID type of this vertex
  *
  * `Edge`
  *   : Concrete edge type for this vertex which implements [`Edge`](com.raphtory.graph.visitor.Edge)
  *
  * `ExplodedEdge`
  *   : Concrete type for this vertex's exploded edges which implements
  *     [`ExplodedEdge`](com.raphtory.graph.visitor.ExplodedEdge)
  *
  * `IDOrdering: Ordering[IDType]`
  *   : implicit ordering object for use when comparing vertex IDs
  *
  * `IDClassTag: ClassTag[IDType]`
  *   : implicit ClassTag object for vertex IDType
  *
  * ## Attributes
  *
  * `ID(): IDType`
  *   : vertex ID
  *
  * `name(nameProperty: String = "name"): String`
  *   : get the name of the vertex
  *
  *     `nameProperty: String = "name"`
  *       : vertex property to use as name (should uniquely identify the vertex)
  *
  *     if `nameProperty` does not exist, this function returns the string representation of the vertex ID
  *
  * `inDegree: Int`
  *   : number of in-neighbours of the vertex
  *
  * `outDegree: Int`
  *   : number of out-neighbours of the vertex
  *
  * `degree: Int`
  *   : total number of neighbours (including in-neighbours and out-neighbours) of the vertex
  *
  * `weightedInDegree[A, B](weightProperty: String = "weight", edgeMergeStrategy: Seq[(Long, A)] => B = PropertyMergeStrategy.sum[A], defaultWeight: A = 1): B`
  *   : sum of incoming edge weights
  *
  *     For the meaning of the input arguments see the
  *     [`Edge.weight` documentation](com.raphtory.graph.visitor.Edge).
  *
  * `weightedOutDegree[A, B](weightProperty: String = "weight", edgeMergeStrategy: Seq[(Long, A)] => B = PropertyMergeStrategy.sum[A], defaultWeight: A = 1): B`
  *   : sum of outgoing edge weights
  *
  *     For the meaning of the input arguments see the
  *     [`Edge.weight` documentation](com.raphtory.graph.visitor.Edge).
  *
  * `weightedTotalDegree[A, B](weightProperty: String = "weight", edgeMergeStrategy: Seq[(Long, A)] => B = PropertyMergeStrategy.sum[A], defaultWeight: A = 1): B`
  *   : sum of incoming and outgoing edge weights
  *
  *     For the meaning of the input arguments see the
  *     [`Edge.weight` documentation](com.raphtory.graph.visitor.Edge).
  *
  * ## Neighbours and Edges
  *
  * `getAllNeighbours(after: Long = Long.MinValue, before: Long = Long.MaxValue): List[IDType]`
  *   : get IDs of all in- and out-neighbours of the vertex
  *
  *     `after: Long = Long.MinValue`
  *       : only return neighbours that are active after time `after`
  *
  *     `before: Long = Long.MaxValue`
  *       : only return neighbours that are active before time `before`
  *
  * `getOutNeighbours(after: Long = Long.MinValue, before: Long = Long.MaxValue): List[IDType]`
  *   : get IDs of all out-neighbours of the vertex
  *
  *     `after: Long = Long.MinValue`
  *       : only return neighbours that are active after time `after`
  *
  *     `before: Long = Long.MaxValue`
  *       : only return neighbours that are active before time `before`
  *
  * `getInNeighbours(after: Long = Long.MinValue, before: Long = Long.MaxValue): List[IDType]`
  *   : get IDs fo all in-neighbours of the vertex
  *
  *     `after: Long = Long.MinValue`
  *       : only return neighbours that are active after time `after`
  *
  *     `before: Long = Long.MaxValue`
  *       : only return neighbours that are active before time `before`
  *
  * `isNeighbour(id: IDType): Boolean`
  *   : check if the vertex with ID `id` is an in- or out-neighbour of this vertex
  *
  * `isInNeighbour(id: IDType): Boolean`
  *   : check if the vertex with ID `id` is an in-neighbour of this vertex
  *
  * `isOutNeighbour(id: IDType): Boolean`
  *   : check if the vertex with ID `id` is an out-neighbour of this vertex
  *
  * `getEdges(after: Long = Long.MinValue, before: Long = Long.MaxValue): List[Edge]`
  *   : return all edges starting or ending at this vertex
  *
  *     `after: Long = Long.MinValue`
  *       : only return edges that are active after time `after`
  *
  *     `before: Long = Long.MaxValue`
  *       : only return edges that are active before time `before`
  *
  * `getOutEdges(after: Long = Long.MinValue, before: Long = Long.MaxValue): List[Edge]`
  *   : return all edges starting at this vertex
  *
  *     `after: Long = Long.MinValue`
  *       : only return edges that are active after time `after`
  *
  *     `before: Long = Long.MaxValue`
  *       : only return edges that are active before time `before`
  *
  * `getInEdges(after: Long = Long.MinValue, before: Long = Long.MaxValue): List[Edge]`
  *   : return all edges ending at this vertex
  *
  *     `after: Long = Long.MinValue`
  *       : only return edges that are active after time `after`
  *
  *     `before: Long = Long.MaxValue`
  *       : only return edges that are active before time `before`
  *
  * `getEdge(id: IDType, after: Long = Long.MinValue, before: Long = Long.MaxValue): Option[Edge]`
  *   : return specified edge if it is an in- or out-edge of this vertex
  *
  *     `id: IDType`
  *       : ID of edge to return
  *
  *     `after: Long = Long.MinValue`
  *       : only return edge if it is active after time `after`
  *
  *     `before: Long = Long.MaxValue`
  *       : only return edge if it is active before time `before`
  *
  * `getOutEdge(id: IDType, after: Long = Long.MinValue, before: Long = Long.MaxValue): Option[Edge]`
  *   : return specified edge if it is an out-edge of this vertex
  *
  *     `id: IDType`
  *       : ID of edge to return
  *
  *     `after: Long = Long.MinValue`
  *       : only return edge if it is active after time `after`
  *
  *     `before: Long = Long.MaxValue`
  *       : only return edge if it is active before time `before`
  *
  * `getInEdge(id: IDType, after: Long = Long.MinValue, before: Long = Long.MaxValue): Option[Edge]`
  *   : return specified edge if it is an in-edge of this vertex
  *
  *     `id: IDType`
  *       : ID of edge to return
  *
  *     `after: Long = Long.MinValue`
  *       : only return edge if it is active after time `after`
  *
  *     `before: Long = Long.MaxValue`
  *       : only return edge if it is active before time `before`
  *
  * `explodeEdges(after: Long = Long.MinValue, before: Long = Long.MaxValue): List[ExplodedEdge]`
  *   : return exploded [`ExplodedEdge`](com.raphtory.graph.visitor.ExplodedEdge) views for each time point
  *     that an in- or out-edge of this vertex is active
  *
  *     `after: Long = Long.MinValue`
  *       : only return views for activity after time `after`
  *
  *     `before: Long = Long.MaxValue`
  *       : only return view for activity before time `before`
  *
  * `explodeOutEdges(after: Long = Long.MinValue, before: Long = Long.MaxValue): List[ExplodedEdge]`
  *   : return exploded [`ExplodedEdge`](com.raphtory.graph.visitor.ExplodedEdge) views for each time point
  *     that an out-edge of this vertex is active
  *
  *     `after: Long = Long.MinValue`
  *       : only return views for activity after time `after`
  *
  *     `before: Long = Long.MaxValue`
  *       : only return view for activity before time `before`
  *
  * `explodeInEdges(after: Long = Long.MinValue, before: Long = Long.MaxValue): List[ExplodedEdge]`
  *   : return exploded [`ExplodedEdge`](com.raphtory.graph.visitor.ExplodedEdge) views for each time point
  *     that an in-edge of this vertex is active
  *
  *     `after: Long = Long.MinValue`
  *       : only return views for activity after time `after`
  *
  *     `before: Long = Long.MaxValue`
  *       : only return view for activity before time `before`
  *
  * `explodeEdge(id: IDType, after: Long = Long.MinValue, before: Long = Long.MaxValue): Option[List[ExplodedEdge]]`
  *   : return exploded [`ExplodedEdge`](com.raphtory.graph.visitor.ExplodedEdge) views for an individual edge
  *     if it is an in- or out-edge of this vertex
  *
  *     `id: IDType`
  *       : ID of edge to explode
  *
  *     `after: Long = Long.MinValue`
  *       : only return views for activity after time `after`
  *
  *     `before: Long = Long.MaxValue`
  *       : only return view for activity before time `before`
  *
  * `explodeOutEdge(id: IDType, after: Long = Long.MinValue, before: Long = Long.MaxValue): Option[List[ExplodedEdge]]`
  *   : return exploded [`ExplodedEdge`](com.raphtory.graph.visitor.ExplodedEdge) views for an individual edge
  *     if it is an out-edge of this vertex
  *
  *     `id: IDType`
  *       : ID of edge to explode
  *
  *     `after: Long = Long.MinValue`
  *       : only return views for activity after time `after`
  *
  *     `before: Long = Long.MaxValue`
  *       : only return view for activity before time `before`
  *
  * `explodeInEdge(id: IDType, after: Long = Long.MinValue, before: Long = Long.MaxValue): Option[List[ExplodedEdge]]`
  *   : return exploded [`ExplodedEdge`](com.raphtory.graph.visitor.ExplodedEdge) views for an individual edge
  *     if it is an in-edge of this vertex
  *
  *     `id: IDType`
  *       : ID of edge to explode
  *
  *     `after: Long = Long.MinValue`
  *       : only return views for activity after time `after`
  *
  *     `before: Long = Long.MaxValue`
  *       : only return view for activity before time `before`
  *
  * ## Messaging
  *
  * `voteToHalt(): Unit`
  *   : vote to stop iterating (iteration stops if all vertices voted to halt)
  *
  * `hasMessage(): Boolean`
  *   : check if vertex has received messages
  *
  * `messageQueue[T]: List[T]`
  *   : queue of received messages
  *
  *     `T`
  *       : message data type
  *
  * `messageSelf(data: Any): Unit`
  *   : send data to this vertex at the next Step/Iteration
  *
  *     `data: Any`
  *       : message data to send
  *
  * `messageVertex(vertexId: IDType, data: Any): Unit`
  *   : send data to another vertex at next Step/Iteration
  *
  *     `vertexID: IDType`
  *       : ID of target vertex for the message
  *
  *     `data: Any`
  *       : message data to send
  *
  * `messageAllNeighbours(message: Any): Unit`
  *   : send the same message data to all in- and out-neighbours of this vertex
  *
  *     `message: Any`
  *       : message data to send
  *
  * `messageOutNeighbours(message: Any): Unit`
  *   : send the same message data to all out-neighbours of this vertex
  *
  *     `message: Any`
  *       : message data to send
  *
  * `messageInNeighbours(message: Any): Unit`
  *   : send the same message data to all in-neighbours of this vertex
  *
  *     `message: Any`
  *       : message data to send
  *
  * ## Algorithmic State
  *
  * `setState(key: String, value: Any): Unit`
  *   : set algorithmic state for this vertex
  *
  *     `key: String`
  *       : key to use for setting value
  *
  *     `value: Any`
  *       : new value for state
  *
  * `getState[T](key: String, includeProperties: Boolean = false): T`
  *   : retrieve value from algorithmic state
  *
  *     `T`
  *       : value type for state
  *
  *     `key: String`
  *       : key to use for retrieving state
  *
  *     `includeProperties: Boolean = false`
  *       : set this to `true` to fall-through to vertex properties if `key` is not found
  *
  * `getStateOrElse[T](key: String, value: T, includeProperties: Boolean = false): T`
  *  : retrieve value from algorithmic state if it exists or return a default value otherwise
  *
  *     `T`
  *       : value type for state
  *
  *     `key: String`
  *       : key to use for retrieving state
  *
  *     `value: T`
  *       : default value to return if state does not exist
  *
  *     `includeProperties: Boolean = false`
  *       : set this to `true` to fall-through to vertex properties if `key` is not found in
  *         algorithmic state
  *
  * `getOrSetState[T](key: String, value: T, includeProperties: Boolean = false): T`
  *   : retrieve value from  algorithmic state if it exists, otherwise set state to default value and return it
  *
  *     `T`
  *       : value type for state
  *
  *     `key: String`
  *       : key to use for retrieving state
  *
  *     `value: T`
  *       : default value to return if state does not exist
  *
  *     `includeProperties: Boolean = false`
  *       : Set this to `true` to fall-through to vertex properties if `key` is not found in
  *         algorithmic state. If the value is pulled in from properties, the new value is set as state.
  *
  * `appendToState[T: ClassTag](key: String, value: T): Unit`
  *   : append new value to existing array or initialise new array if state does not exist
  *
  *     The value type of the state is assumed to be `Array[T]` if the state already exists.
  *
  *     `T`
  *       : value type for state (needs to have a `ClassTag` available due to Scala `Array` implementation)
  *
  *     `key: String`
  *       : key to use for retrieving state
  *
  *     `value: T`
  *       : value to append to state
  *
  * `containsState(key: String, includeProperties: Boolean = false): Boolean`
  *   : check if algorithmic state with key `key` exists
  *
  *     `includeProperties: Boolean = false`
  *       : Set this to `true` to fall-through to vertex properties if `key` is not found.
  *         If set, this function only returns `false` if `key` is not included in either algorithmic state
  *         or vertex properties
  */
trait Vertex extends EntityVisitor {
  type IDType
  type Edge <: visitor.ConcreteEdge[IDType]
  type ExplodedEdge = visitor.ConcreteExplodedEdge[IDType]
  implicit val IDOrdering: Ordering[IDType]
  implicit val IDClassTag: ClassTag[IDType] = ClassTag[IDType](ID().getClass)

  def ID(): IDType

  def name(nameProperty: String = "name"): String =
    getPropertyOrElse[String](nameProperty, ID.toString)

  //functionality for checking messages
  def hasMessage(): Boolean
  def messageQueue[T]: List[T]
  def voteToHalt(): Unit
  //Send message
  def messageSelf(data: Any): Unit = messageVertex(ID(), data)
  def messageVertex(vertexId: IDType, data: Any): Unit

  def messageOutNeighbours(message: Any): Unit =
    getOutNeighbours().foreach(messageVertex(_, message))

  def messageAllNeighbours(message: Any): Unit =
    getAllNeighbours().foreach(messageVertex(_, message))
  def messageInNeighbours(message: Any): Unit  = getInNeighbours().foreach(messageVertex(_, message))

  //Get Neighbours
  def getOutNeighbours(after: Long = Long.MinValue, before: Long = Long.MaxValue): List[IDType] =
    getOutEdges(after, before).map(_.dst())

  def getInNeighbours(after: Long = Long.MinValue, before: Long = Long.MaxValue): List[IDType] =
    getInEdges(after, before).map(_.src())

  def getAllNeighbours(after: Long = Long.MinValue, before: Long = Long.MaxValue): List[IDType] =
    (getInNeighbours(after, before) ++ getOutNeighbours(after, before)).distinct

  //Check Neighbours
  private lazy val inNeighbourSet  = getInNeighbours().toSet
  private lazy val outNeighbourSet = getOutNeighbours().toSet

  def isNeighbour(id: IDType): Boolean    =
    inNeighbourSet.contains(id) || outNeighbourSet.contains(id)
  def isInNeighbour(id: IDType): Boolean  = inNeighbourSet.contains(id)
  def isOutNeighbour(id: IDType): Boolean = outNeighbourSet.contains(id)

  //Degree
  lazy val degree: Int    = getAllNeighbours().size
  lazy val outDegree: Int = getOutNeighbours().size
  lazy val inDegree: Int  = getInNeighbours().size

  //all edges
  def getEdges(after: Long = Long.MinValue, before: Long = Long.MaxValue): List[Edge]
  //all out edges
  def getOutEdges(after: Long = Long.MinValue, before: Long = Long.MaxValue): List[Edge]
  //all in edges
  def getInEdges(after: Long = Long.MinValue, before: Long = Long.MaxValue): List[Edge]

  //individual out edge
  def getOutEdge(
      id: IDType,
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): Option[Edge]

  //individual in edge
  def getInEdge(
      id: IDType,
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): Option[Edge]

  //get individual edge irrespective of direction
  def getEdge(
      id: IDType,
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): Option[Edge]

  //all edges
  def explodeEdges(
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): List[ExplodedEdge] =
    getEdges(after, before).flatMap(_.explode())

  //all out edges
  def explodeOutEdges(
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): List[ExplodedEdge] =
    getOutEdges(after, before).flatMap(_.explode())

  //all in edges
  def explodeInEdges(
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): List[ExplodedEdge] =
    getInEdges(after, before).flatMap(_.explode())

  //individual out edge
  def explodeOutEdge(
      id: IDType,
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): Option[List[ExplodedEdge]] =
    getOutEdge(id, after, before).map(_.explode())

  //individual in edge
  def explodeInEdge(
      id: IDType,
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): Option[List[ExplodedEdge]] =
    getInEdge(id, after, before).map(_.explode())

  //individual edge
  def explodedEdge(
      id: IDType,
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): Option[List[ExplodedEdge]] =
    getEdge(id, after, before).map(_.explode())

  // analytical state
  def setState(key: String, value: Any): Unit
  // if includeProperties = true, key is looked up first in analytical state with a fall-through to properties if not found
  def getState[T](key: String, includeProperties: Boolean = false): T
  def getStateOrElse[T](key: String, value: T, includeProperties: Boolean = false): T
  def containsState(key: String, includeProperties: Boolean = false): Boolean
  // if includeProperties = true and value is pulled in from properties, the new value is set as state
  def getOrSetState[T](key: String, value: T, includeProperties: Boolean = false): T
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
        getInEdges()
      case EdgeDirection.Outgoing =>
        getOutEdges()
      case EdgeDirection.Both     =>
        getEdges()
    })
      .map(_.weight(weightProperty, edgeMergeStrategy, defaultWeight))
      .sum

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

  def weightedInDegree[A: Numeric, B: Numeric](
      weightProperty: String,
      edgeMergeStrategy: PropertyMerge[A, B]
  ): B =
    weightedInDegree(weightProperty, edgeMergeStrategy, DefaultValues.defaultVal[A])

  def weightedInDegree[A: Numeric, B: Numeric](
      edgeMergeStrategy: PropertyMerge[A, B]
  ): B =
    weightedInDegree(DefaultValues.weightProperty, edgeMergeStrategy, DefaultValues.defaultVal[A])

  def weightedInDegree[A: Numeric](
      weightProperty: String,
      defaultWeight: A
  ): A =
    weightedInDegree(weightProperty, DefaultValues.mergeStrategy[A], defaultWeight)

  def weightedInDegree[A: Numeric](
      defaultWeight: A
  ): A =
    weightedInDegree(DefaultValues.weightProperty, DefaultValues.mergeStrategy[A], defaultWeight)

  def weightedInDegree[A: Numeric](
      weightProperty: String
  ): A =
    weightedInDegree(weightProperty, DefaultValues.mergeStrategy[A], DefaultValues.defaultVal[A])

  def weightedInDegree[A: Numeric](
  ): A =
    weightedInDegree(
            DefaultValues.weightProperty,
            DefaultValues.mergeStrategy[A],
            DefaultValues.defaultVal[A]
    )

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

  def weightedOutDegree[A: Numeric, B: Numeric](
      weightProperty: String,
      edgeMergeStrategy: PropertyMerge[A, B]
  ): B =
    weightedOutDegree(weightProperty, edgeMergeStrategy, DefaultValues.defaultVal[A])

  def weightedOutDegree[A: Numeric, B: Numeric](
      edgeMergeStrategy: PropertyMerge[A, B]
  ): B =
    weightedOutDegree(DefaultValues.weightProperty, edgeMergeStrategy, DefaultValues.defaultVal[A])

  def weightedOutDegree[A: Numeric](
      weightProperty: String,
      defaultWeight: A
  ): A =
    weightedOutDegree(weightProperty, DefaultValues.mergeStrategy[A], defaultWeight)

  def weightedOutDegree[A: Numeric](
      defaultWeight: A
  ): A =
    weightedOutDegree(
            DefaultValues.weightProperty,
            DefaultValues.mergeStrategy[A],
            defaultWeight
    )

  def weightedOutDegree[A: Numeric](
      weightProperty: String
  ): A =
    weightedOutDegree(weightProperty, DefaultValues.mergeStrategy[A], DefaultValues.defaultVal[A])

  def weightedOutDegree[A: Numeric](
  ): A =
    weightedOutDegree(
            DefaultValues.weightProperty,
            DefaultValues.mergeStrategy[A],
            DefaultValues.defaultVal[A]
    )

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

  def weightedTotalDegree[A: Numeric, B: Numeric](
      weightProperty: String,
      edgeMergeStrategy: PropertyMerge[A, B]
  ): B =
    weightedTotalDegree(weightProperty, edgeMergeStrategy, DefaultValues.defaultVal[A])

  def weightedTotalDegree[A: Numeric, B: Numeric](
      edgeMergeStrategy: PropertyMerge[A, B]
  ): B =
    weightedTotalDegree[A, B](
            DefaultValues.weightProperty,
            edgeMergeStrategy,
            DefaultValues.defaultVal[A]
    )

  def weightedTotalDegree[A: Numeric](
      weightProperty: String,
      defaultWeight: A
  ): A =
    weightedTotalDegree(weightProperty, DefaultValues.mergeStrategy[A], defaultWeight)

  def weightedTotalDegree[A: Numeric](
      defaultWeight: A
  ): A =
    weightedTotalDegree(DefaultValues.weightProperty, DefaultValues.mergeStrategy[A], defaultWeight)

  def weightedTotalDegree[A: Numeric](
      weightProperty: String
  ): A =
    weightedTotalDegree(weightProperty, DefaultValues.mergeStrategy[A], DefaultValues.defaultVal[A])

  def weightedTotalDegree[A: Numeric](
  ): A =
    weightedTotalDegree(
            DefaultValues.weightProperty,
            DefaultValues.mergeStrategy[A],
            DefaultValues.defaultVal[A]
    )

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
