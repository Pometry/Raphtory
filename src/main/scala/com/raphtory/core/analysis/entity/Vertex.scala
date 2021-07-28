package com.raphtory.core.analysis.entity

import com.raphtory.core.analysis.GraphLens
import com.raphtory.core.model.communication._
import com.raphtory.core.model.entities.RaphtoryVertex

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ParIterable
import scala.collection.parallel.mutable.ParTrieMap
import scala.reflect.ClassTag

final case class Vertex(
    private val v: RaphtoryVertex,
    private val internalIncomingEdges: ParTrieMap[Long, Edge],
    private val internalOutgoingEdges: ParTrieMap[Long, Edge],
    private val lens: GraphLens
) extends EntityVisitor(v, lens) {

  def ID() = v.vertexId

  private val multiQueue: VertexMultiQueue        = new VertexMultiQueue() //Map of queues for all ongoing processing
  private var computationValues: Map[String, Any] = Map.empty              //Partial results kept between supersteps in calculation

  def hasMessage(): Boolean = multiQueue.getMessageQueue(lens.superStep).nonEmpty

  def messageQueue[T: ClassTag]: List[T] = { //clears queue after getting it to make sure not there for next iteration
    val queue = multiQueue.getMessageQueue(lens.superStep).map(_.asInstanceOf[T])
    multiQueue.clearQueue(lens.superStep)
    queue
  }

  def voteToHalt()         : Unit                                = lens.vertexVoted()
  def aliveAt(time: Long): Boolean                         = v.aliveAt(time)
  def aliveAtWithWindow(time: Long, window: Long): Boolean = v.aliveAtWithWindow(time, window)

  //out edges whole
  def getOutEdges: ParIterable[Edge]                              = internalOutgoingEdges.map(x=>x._2)
  def getOutEdgesAfter(time: Long): ParIterable[Edge]             = getOutEdges.filter(_.activityAfter(time))
  def getOutEdgesBefore(time: Long): ParIterable[Edge]            = getOutEdges.filter(_.activityBefore(time))
  def getOutEdgesBetween(min: Long, max: Long): ParIterable[Edge] = getOutEdges.filter(_.activityBetween(min, max))

  //in edges whole
  def getIncEdges: ParIterable[Edge]                              = internalIncomingEdges.map(x=>x._2)
  def getIncEdgesAfter(time: Long): ParIterable[Edge]             = getIncEdges.filter(_.activityAfter(time))
  def getInCEdgesBefore(time: Long): ParIterable[Edge]            = getIncEdges.filter(_.activityBefore(time))
  def getInCEdgesBetween(min: Long, max: Long): ParIterable[Edge] = getIncEdges.filter(_.activityBetween(min, max))

  //out edges individual
  def getOutEdge(id: Long): Option[Edge]                  = internalOutgoingEdges.get(id)
  def getOutEdgeAfter(id: Long, time: Long): Option[Edge] = internalOutgoingEdges.get(id).filter(_.activityAfter(time))
  def getOutEdgeBefore(id: Long, time: Long): Option[Edge] =
    internalOutgoingEdges.get(id).filter(_.activityBefore(time))
  def getOutEdgeBetween(id: Long, min: Long, max: Long): Option[Edge] =
    internalOutgoingEdges.get(id).filter(_.activityBetween(min, max))

  //In edges individual
  def getInEdge(id: Long): Option[Edge]                   = internalIncomingEdges.get(id)
  def getInEdgeAfter(id: Long, time: Long): Option[Edge]  = internalIncomingEdges.get(id).filter(_.activityAfter(time))
  def getInEdgeBefore(id: Long, time: Long): Option[Edge] = internalIncomingEdges.get(id).filter(_.activityBefore(time))
  def getInEdgeBetween(id: Long, min: Long, max: Long): Option[Edge] =
    internalIncomingEdges.get(id).filter(_.activityBetween(min, max))

  // state related
  def setState(key: String, value: Any): Unit =
    computationValues += ((key, value))
  def getState[T: ClassTag](key: String) =
    computationValues(key).asInstanceOf[T]
  def containsState(key: String): Boolean =
    computationValues.contains(key)
  def getOrSetState[T: ClassTag](key: String, value: T):T =
    computationValues.get(key) match {
      case Some(value) =>
        value.asInstanceOf[T]
      case None =>
        setState(key, value)
        value
    }

  def appendToState[T: ClassTag](key: String, value: Any) = //write function later
    computationValues.get(key) match {
      case Some(arr) =>
        setState(key, arr.asInstanceOf[Array[Any]] :+ value)
      case None =>
        setState(key, Array(value))
        value
    }

  //Send message
  def messageNeighbour(vertexId: Long, data: Any): Unit =
    lens.sendMessage(VertexMessage(vertexId, data))

  def messageAllOutgoingNeighbors(message: Any): Unit =
    internalOutgoingEdges.keys.foreach(vId => messageNeighbour(vId, message))

  def messageAllNeighbours(message: Any) =
    internalOutgoingEdges.keySet.union(internalIncomingEdges.keySet).foreach(vId => messageNeighbour(vId, message))

  def messageAllIngoingNeighbors(message: Any): Unit =
    internalIncomingEdges.keys.foreach(vId => messageNeighbour(vId, message))

  // todo hide
  def receiveMessage(msg: VertexMessage): Unit = {
    multiQueue.receiveMessage(lens.superStep, msg.data)
  }
}
