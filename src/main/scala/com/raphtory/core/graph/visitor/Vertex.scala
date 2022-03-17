package com.raphtory.core.graph.visitor

import PropertyMergeStrategy.PropertyMerge
import com.raphtory.core.graph.visitor.EdgeDirection.Direction

trait Vertex extends EntityVisitor {

  def ID(): Long

  def name(nameProperty: String = "name"): String =
    getPropertyOrElse[String](nameProperty, ID.toString)

  //functionality for checking messages
  def hasMessage(): Boolean
  def messageQueue[T]: List[T]
  def voteToHalt(): Unit
  //Send message
  def messageSelf(data: Any): Unit
  def messageVertex(vertexId: Long, data: Any): Unit
  def messageOutNeighbours(message: Any): Unit
  def messageAllNeighbours(message: Any): Unit
  def messageInNeighbours(message: Any): Unit

  //Get Neighbours
  def getAllNeighbours(after: Long = 0L, before: Long = Long.MaxValue): List[Long]
  def getOutNeighbours(after: Long = 0L, before: Long = Long.MaxValue): List[Long]
  def getInNeighbours(after: Long = 0L, before: Long = Long.MaxValue): List[Long]

  //Check Neighbours
  private lazy val inNeighbourSet  = getInNeighbours().toSet
  private lazy val outNeighbourSet = getOutNeighbours().toSet

  def isNeighbour(id: Long): Boolean    = inNeighbourSet.contains(id) || outNeighbourSet.contains(id)
  def isInNeighbour(id: Long): Boolean  = inNeighbourSet.contains(id)
  def isOutNeighbour(id: Long): Boolean = outNeighbourSet.contains(id)

  //Degree
  lazy val degree: Int    = getAllNeighbours().size
  lazy val outDegree: Int = getOutNeighbours().size
  lazy val inDegree: Int  = getInNeighbours().size

  //all edges
  def getEdges(after: Long = 0L, before: Long = Long.MaxValue): List[Edge]
  //all out edges
  def getOutEdges(after: Long = 0L, before: Long = Long.MaxValue): List[Edge]
  //all in edges
  def getInEdges(after: Long = 0L, before: Long = Long.MaxValue): List[Edge]
  //individual out edge
  def getOutEdge(id: Long, after: Long = 0L, before: Long = Long.MaxValue): Option[Edge]
  //individual in edge
  def getInEdge(id: Long, after: Long = 0L, before: Long = Long.MaxValue): Option[Edge]
  //get individual edge irrespective of direction
  def getEdge(id: Long, after: Long = 0L, before: Long = Long.MaxValue): Option[Edge]

  //all edges
  def explodeEdges(after: Long = 0L, before: Long = Long.MaxValue): List[ExplodedEdge] =
    getEdges(after, before).flatMap(_.explode())

  //all out edges
  def explodeOutEdges(after: Long = 0L, before: Long = Long.MaxValue): List[ExplodedEdge] =
    getOutEdges(after, before).flatMap(_.explode())

  //all in edges
  def explodeInEdges(after: Long = 0L, before: Long = Long.MaxValue): List[ExplodedEdge] =
    getInEdges(after, before).flatMap(_.explode())

  //individual out edge
  def explodeOutEdge(
      id: Long,
      after: Long = 0L,
      before: Long = Long.MaxValue
  ): Option[List[ExplodedEdge]] =
    getOutEdge(id, after, before).map(_.explode())

  //individual in edge
  def explodeInEdge(
      id: Long,
      after: Long = 0L,
      before: Long = Long.MaxValue
  ): Option[List[ExplodedEdge]] =
    getInEdge(id, after, before).map(_.explode())

  //individual edge
  def explodedEdge(
      id: Long,
      after: Long = 0L,
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
  def appendToState[T](key: String, value: Any): Unit

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

  // Also need a function for receiving messages, but the user should not have access to this
  //private def receiveMessage(msg: VertexMessage): Unit
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
