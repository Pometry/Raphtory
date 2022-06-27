package com.raphtory.internals.storage.pojograph.entities.external

import com.raphtory.api.analysis.visitor.Vertex
import com.raphtory.internals.components.querymanager.FilteredEdgeMessage
import com.raphtory.internals.components.querymanager.FilteredInEdgeMessage
import com.raphtory.internals.components.querymanager.FilteredOutEdgeMessage
import com.raphtory.internals.components.querymanager.GenericVertexMessage
import com.raphtory.internals.components.querymanager.VertexMessage
import com.raphtory.internals.storage.pojograph.PojoGraphLens
import com.raphtory.internals.storage.pojograph.messaging.VertexMultiQueue

import scala.collection.mutable

private[raphtory] trait PojoVertexBase extends Vertex {
  // abstract state
  protected def lens: PojoGraphLens
  protected val internalIncomingEdges: mutable.Map[IDType, Edge]
  protected val internalOutgoingEdges: mutable.Map[IDType, Edge]

  // queues
  protected var multiQueue: VertexMultiQueue =
    new VertexMultiQueue() //Map of queues for all ongoing processing
  protected val incomingEdgeDeleteMultiQueue: VertexMultiQueue = new VertexMultiQueue()
  protected val outgoingEdgeDeleteMultiQueue: VertexMultiQueue = new VertexMultiQueue()

  // messaging
  def hasMessage: Boolean =
    multiQueue.getMessageQueue(lens.superStep).nonEmpty

  def messageQueue[T]: List[T] = { //clears queue after getting it to make sure not there for next iteration
    val queue = multiQueue.getMessageQueue(lens.superStep).map(_.asInstanceOf[T])
    multiQueue.clearQueue(lens.superStep)
    queue
  }

  def clearMessageQueue(): Unit =
    multiQueue = new VertexMultiQueue()

  def voteToHalt(): Unit = lens.vertexVoted()

  //Send message
  override def messageSelf(data: Any): Unit =
    lens.sendMessage(VertexMessage(lens.superStep + 1, ID, data))

  def messageVertex(vertexId: IDType, data: Any): Unit = {
    val message = VertexMessage(lens.superStep + 1, vertexId, data)
    lens.sendMessage(message)
  }

  override def messageOutNeighbours(message: Any): Unit =
    internalOutgoingEdges.keys.foreach(vId => messageVertex(vId, message))

  override def messageAllNeighbours(message: Any): Unit =
    internalOutgoingEdges.keySet
      .union(internalIncomingEdges.keySet)
      .foreach(vId => messageVertex(vId, message))

  override def messageInNeighbours(message: Any): Unit =
    internalIncomingEdges.keys.foreach(vId => messageVertex(vId, message))

  def receiveMessage(msg: GenericVertexMessage[_]): Unit =
    msg match {
      case msg: VertexMessage[_, _]       => multiQueue.receiveMessage(msg.superstep, msg.data)
      case msg: FilteredOutEdgeMessage[_] =>
        lens.needsFiltering = true
        outgoingEdgeDeleteMultiQueue.receiveMessage(msg.superstep, msg.sourceId)
      case msg: FilteredInEdgeMessage[_]  =>
        lens.needsFiltering = true
        incomingEdgeDeleteMultiQueue.receiveMessage(msg.superstep, msg.sourceId)
      case msg: FilteredEdgeMessage[_]    =>
        lens.needsFiltering = true
        outgoingEdgeDeleteMultiQueue.receiveMessage(msg.superstep, msg.sourceId)
        incomingEdgeDeleteMultiQueue.receiveMessage(msg.superstep, msg.sourceId)
    }

  //filtering
  private var filtered = false

  def executeEdgeDelete(): Unit = {
    internalOutgoingEdges --= outgoingEdgeDeleteMultiQueue
      .getMessageQueue(lens.superStep)
      .map(_.asInstanceOf[IDType])
    internalIncomingEdges --= incomingEdgeDeleteMultiQueue
      .getMessageQueue(lens.superStep)
      .map(_.asInstanceOf[IDType])
    outgoingEdgeDeleteMultiQueue.clearQueue(lens.superStep)
    incomingEdgeDeleteMultiQueue.clearQueue(lens.superStep)
  }

  def isFiltered = filtered

  def remove(): Unit = {
    // key is the vertex of the other side of edge
    filtered = true
    lens.needsFiltering = true
    internalIncomingEdges.keys.foreach(k => lens.sendMessage(FilteredOutEdgeMessage(lens.superStep + 1, k, ID)))
    internalOutgoingEdges.keys.foreach(k => lens.sendMessage(FilteredInEdgeMessage(lens.superStep + 1, k, ID)))
  }

  def getOutEdges(after: Long = Long.MinValue, before: Long = Long.MaxValue): List[Edge] =
    allEdge(internalOutgoingEdges, after, before)

  //in edges whole
  def getInEdges(after: Long = Long.MinValue, before: Long = Long.MaxValue): List[Edge] =
    allEdge(internalIncomingEdges, after, before)

  //all edges
  def getAllEdges(after: Long = Long.MinValue, before: Long = Long.MaxValue): List[Edge] =
    getInEdges(after, before) ++ getOutEdges(after, before)

  //out edges individual
  def getOutEdge(
      id: IDType,
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): Option[Edge] =
    individualEdge(internalOutgoingEdges, after, before, id)

  //In edges individual
  def getInEdge(
      id: IDType,
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): Option[Edge] =
    individualEdge(internalIncomingEdges, after, before, id)

  override def getEdge(id: IDType, after: Long, before: Long): List[Edge] =
    List(getInEdge(id, after, before), getOutEdge(id, after, before)).flatten

  def allEdge(
      edges: mutable.Map[IDType, Edge],
      after: Long,
      before: Long
  ): List[Edge] =
    if (after <= lens.start && before >= lens.end)
      edges.values.toList
    else
      edges.collect {
        case (_, edge) if edge.active(after, before) => edge
      }.toList

  def individualEdge(
      edges: mutable.Map[IDType, Edge],
      after: Long,
      before: Long,
      id: IDType
  ): Option[Edge] =
    if (after <= lens.start && before >= lens.end)
      edges.get(id)
    else
      edges.get(id) match {
        case Some(edge) => if (edge.active(after, before)) Some(edge) else None
        case None       => None
      }
}
