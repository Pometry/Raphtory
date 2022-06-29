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
  override type Edge <: PojoExEdgeBase[IDType]
  def lens: PojoGraphLens

  // messaging
  def hasMessage: Boolean

  def messageQueue[T]: List[T]

  def clearMessageQueue(): Unit

  def voteToHalt(): Unit = lens.vertexVoted()

  //Send message
  override def messageSelf(data: Any): Unit =
    lens.sendMessage(VertexMessage(lens.superStep + 1, ID, data))

  def messageVertex(vertexId: IDType, data: Any): Unit = {
    val message = VertexMessage(lens.superStep + 1, vertexId, data)
    lens.sendMessage(message)
  }

  def receiveMessage(msg: GenericVertexMessage[_]): Unit

  def executeEdgeDelete(): Unit

  def isFiltered: Boolean

  override def getEdge(id: IDType): List[Edge] =
    List(getInEdge(id), getOutEdge(id)).flatten
}

private[raphtory] trait PojoConcreteVertexBase[T] extends PojoVertexBase {
  // abstract state
  override type IDType = T
  override type Edge <: PojoExDirectedEdgeBase[Edge, IDType]
  def lens: PojoGraphLens
  val internalIncomingEdges: mutable.Map[IDType, Edge]
  val internalOutgoingEdges: mutable.Map[IDType, Edge]

  // queues
  val multiQueue: VertexMultiQueue =
    new VertexMultiQueue() //Map of queues for all ongoing processing
  protected val incomingEdgeDeleteMultiQueue: VertexMultiQueue = new VertexMultiQueue()
  protected val outgoingEdgeDeleteMultiQueue: VertexMultiQueue = new VertexMultiQueue()

  def viewUndirected: PojoUndirectedVertexView[T]

  // messaging
  def hasMessage: Boolean =
    multiQueue.getMessageQueue(lens.superStep).nonEmpty

  def messageQueue[T]: List[T] = { //clears queue after getting it to make sure not there for next iteration
    val queue = multiQueue.getMessageQueue(lens.superStep).map(_.asInstanceOf[T])
    multiQueue.clearQueue(lens.superStep)
    queue
  }

  def clearMessageQueue(): Unit =
    multiQueue.clearAll()

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

  def isFiltered: Boolean = filtered

  def remove(): Unit = {
    // key is the vertex of the other side of edge
    filtered = true
    lens.needsFiltering = true
    internalIncomingEdges.keys.foreach(k => lens.sendMessage(FilteredOutEdgeMessage(lens.superStep + 1, k, ID)))
    internalOutgoingEdges.keys.foreach(k => lens.sendMessage(FilteredInEdgeMessage(lens.superStep + 1, k, ID)))
  }

  def outEdges: List[Edge] = internalOutgoingEdges.values.toList

  def inEdges: List[Edge] = internalIncomingEdges.values.toList

  //out edges individual
  def getOutEdge(
      id: IDType
  ): Option[Edge] =
    internalOutgoingEdges.get(id)

  //In edges individual
  def getInEdge(
      id: IDType
  ): Option[Edge] =
    internalIncomingEdges.get(id)
}
