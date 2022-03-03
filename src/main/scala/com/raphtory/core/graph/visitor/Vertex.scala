package com.raphtory.core.graph.visitor

import com.raphtory.core.algorithm.MergeStrategy
import com.raphtory.core.algorithm.MergeStrategy.Merge
import com.raphtory.core.graph.visitor.EdgeDirection.Direction

import scala.reflect.ClassTag

trait Vertex extends EntityVisitor {

  def ID(): Long
  def name(nameProperty: String = "name"): String = getPropertyOrElse(nameProperty, ID.toString)

  //functionality for checking messages
  def hasMessage(): Boolean
  def messageQueue[T: ClassTag]: List[T]
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
  def explodeEdges(after: Long = 0L, before: Long = Long.MaxValue): List[ExplodedEdge]
  //all out edges
  def explodeOutEdges(after: Long = 0L, before: Long = Long.MaxValue): List[ExplodedEdge]
  //all in edges
  def explodeInEdges(after: Long = 0L, before: Long = Long.MaxValue): List[ExplodedEdge]

  //individual out edge
  def explodeOutEdge(
      id: Long,
      after: Long = 0L,
      before: Long = Long.MaxValue
  ): Option[List[ExplodedEdge]]

  //individual in edge
  def explodeInEdge(
      id: Long,
      after: Long = 0L,
      before: Long = Long.MaxValue
  ): Option[List[ExplodedEdge]]

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
  private def directedEdgeWeight(dir : Direction = EdgeDirection.Incoming, weightProperty : String="weight",
                                 edgeMergeStrategy : Merge = MergeStrategy.Sum, vertexMergeStrategy: Merge=MergeStrategy.Sum,
                                 defaultWeight:Float=1.0f) : Float = {
    val eWeights = ( dir match {
      case EdgeDirection.Incoming =>
        getInEdges()
      case EdgeDirection.Outgoing =>
        getOutEdges()
      case EdgeDirection.Both =>
        getEdges()
    })
      .map(e=>(e, e.totalWeight(edgeMergeStrategy, weightProperty, defaultWeight)))
    vertexMergeStrategy match {
      case MergeStrategy.Sum =>
        eWeights.map{case (edge, weight) => weight}.sum
      case MergeStrategy.Min =>
        eWeights.map{case (edge, weight) => weight}.min
      case MergeStrategy.Max =>
        eWeights.map{case (edge, weight) => weight}.max
      case MergeStrategy.Product =>
        eWeights.map{case (edge, weight) => weight}.product
      case MergeStrategy.Average =>
        val wgts = eWeights.map{case (edge, weight) => weight}
        if (wgts.nonEmpty) wgts.sum/wgts.size else 0.0f
      case MergeStrategy.Earliest =>
        val (earliestEdge, earliestWeight) = eWeights.minBy{case (edge, weight) => edge.latestActivity().time}
        earliestWeight
      case MergeStrategy.Latest =>
        val (latestEdge, latestWeight) = eWeights.maxBy{case (edge, weight) => edge.latestActivity().time}
        latestWeight
    }
  }

  def inWeight(weightProperty : String="weight", edgeMergeStrategy : Merge = MergeStrategy.Sum,
               vertexMergeStrategy : Merge = MergeStrategy.Sum, defaultWeight:Float=1.0f) : Float = {
    directedEdgeWeight(EdgeDirection.Incoming,weightProperty,edgeMergeStrategy, vertexMergeStrategy, defaultWeight)
  }
  def outWeight(weightProperty : String="weight", edgeMergeStrategy : Merge = MergeStrategy.Sum,
                vertexMergeStrategy : Merge = MergeStrategy.Sum, defaultWeight:Float=1.0f) : Float = {
    directedEdgeWeight(EdgeDirection.Outgoing,weightProperty,edgeMergeStrategy, vertexMergeStrategy, defaultWeight)
  }
  def totalWeight(weightProperty : String="weight", edgeMergeStrategy : Merge = MergeStrategy.Sum,
                  vertexMergeStrategy : Merge = MergeStrategy.Sum, defaultWeight:Float=1.0f)  : Float = {
    directedEdgeWeight(EdgeDirection.Both,weightProperty,edgeMergeStrategy, vertexMergeStrategy, defaultWeight)
  }
  def weightBalance(weightProperty:String="weight", defaultWeight:Float=1.0f) : Float = {
    inWeight(weightProperty, defaultWeight = defaultWeight) - outWeight(weightProperty, defaultWeight = defaultWeight)
  }

  // Also need a function for receiving messages, but the user should not have access to this
  //private def receiveMessage(msg: VertexMessage): Unit
}
