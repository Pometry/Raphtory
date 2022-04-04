package com.raphtory.storage.pojograph.entities.external

import com.raphtory.components.querymanager.FilteredEdgeMessage
import com.raphtory.components.querymanager.FilteredInEdgeMessage
import com.raphtory.components.querymanager.FilteredOutEdgeMessage
import com.raphtory.components.querymanager.GenericVertexMessage
import com.raphtory.components.querymanager.VertexMessage
import com.raphtory.graph.visitor.Vertex
import com.raphtory.storage.pojograph.PojoGraphLens
import com.raphtory.storage.pojograph.entities.internal.PojoVertex
import com.raphtory.storage.pojograph.messaging.VertexMultiQueue

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.math.Ordering

/** @DoNotDocument */
class PojoExVertex(
    val v: PojoVertex,
    private val internalIncomingEdges: mutable.Map[Long, PojoExEdge],
    private val internalOutgoingEdges: mutable.Map[Long, PojoExEdge],
    val lens: PojoGraphLens
) extends PojoExEntity(v, lens)
        with Vertex {

  override type VertexID = Long
  implicit override val ordering = Ordering.Long
  override def ID(): Long        = v.vertexId

  private var multiQueue: VertexMultiQueue =
    new VertexMultiQueue() //Map of queues for all ongoing processing
  private var computationValues: Map[String, Any] =
    Map.empty //Partial results kept between supersteps in calculation

  var exploded = Map.empty[Long, PojoExplodedVertex]

  def hasMessage(): Boolean =
    multiQueue.getMessageQueue(lens.superStep).nonEmpty

  def messageQueue[T]
      : List[T] = { //clears queue after getting it to make sure not there for next iteration
    val queue = multiQueue.getMessageQueue(lens.superStep).map(_.asInstanceOf[T])
    multiQueue.clearQueue(lens.superStep)
    queue
  }

  private val incomingEdgeDeleteMultiQueue: VertexMultiQueue = new VertexMultiQueue()
  private val outgoingEdgeDeleteMultiQueue: VertexMultiQueue = new VertexMultiQueue()
  private var filtered                                       = false

  def executeEdgeDelete(): Unit = {
    internalOutgoingEdges --= outgoingEdgeDeleteMultiQueue
      .getMessageQueue(lens.superStep)
      .map(_.asInstanceOf[Long])
    internalIncomingEdges --= incomingEdgeDeleteMultiQueue
      .getMessageQueue(lens.superStep)
      .map(_.asInstanceOf[Long])
    outgoingEdgeDeleteMultiQueue.clearQueue(lens.superStep)
    incomingEdgeDeleteMultiQueue.clearQueue(lens.superStep)
  }

  def clearMessageQueue(): Unit =
    multiQueue = new VertexMultiQueue()

  def voteToHalt(): Unit = lens.vertexVoted()

  //out edges whole
  def getOutEdges(after: Long = 0L, before: Long = Long.MaxValue): List[PojoExEdge] =
    allEdge(internalOutgoingEdges, after, before)

  //in edges whole
  def getInEdges(after: Long = 0L, before: Long = Long.MaxValue): List[PojoExEdge] =
    allEdge(internalIncomingEdges, after, before)

  //all edges
  def getEdges(after: Long = 0L, before: Long = Long.MaxValue): List[PojoExEdge] =
    getInEdges(after, before) ++ getOutEdges(after, before)

  //out edges individual
  def getOutEdge(id: Long, after: Long = 0L, before: Long = Long.MaxValue): Option[PojoExEdge] =
    individualEdge(internalOutgoingEdges, after, before, id)

  //In edges individual
  def getInEdge(id: Long, after: Long = 0L, before: Long = Long.MaxValue): Option[PojoExEdge] =
    individualEdge(internalIncomingEdges, after, before, id)

  // edge individual
  def getEdge(id: Long, after: Long = 0L, before: Long = Long.MaxValue): Option[PojoExEdge] =
    individualEdge(internalIncomingEdges ++ internalOutgoingEdges, after, before, id)

  private def allEdge(
      edges: mutable.Map[Long, PojoExEdge],
      after: Long,
      before: Long
  ): List[PojoExEdge] =
    if (after == 0 && before == Long.MaxValue)
      edges.values.toList
    else
      edges.collect {
        case (_, edge) if edge.active(after, before) => edge
      }.toList

  private def individualEdge(
      edges: mutable.Map[Long, PojoExEdge],
      after: Long,
      before: Long,
      id: Long
  ) =
    if (after == 0 && before == Long.MaxValue)
      edges.get(id)
    else
      edges.get(id) match {
        case Some(edge) => if (edge.active(after, before)) Some(edge) else None
        case None       => None
      }

  // state related
  def setState(key: String, value: Any): Unit =
    computationValues += ((key, value))

  def getState[T](key: String, includeProperties: Boolean = false): T =
    if (computationValues.contains(key))
      computationValues(key).asInstanceOf[T]
    else if (includeProperties && v.properties.contains(key))
      getProperty[T](key).get
    else if (includeProperties)
      throw new Exception(
              s"$key not found within analytical state or properties for vertex ${v.vertexId}"
      )
    else
      throw new Exception(s"$key not found within analytical state for vertex ${v.vertexId}")

  def getStateOrElse[T](key: String, value: T, includeProperties: Boolean = false): T =
    if (computationValues contains key)
      computationValues(key).asInstanceOf[T]
    else if (includeProperties && v.properties.contains(key))
      getProperty[T](key).get
    else
      value

  def containsState(key: String, includeProperties: Boolean = false): Boolean =
    computationValues.contains(key) || (includeProperties && v.properties.contains(key))

  def getOrSetState[T](key: String, value: T, includeProperties: Boolean = false): T = {
    var output_value = value
    if (containsState(key))
      output_value = getState[T](key)
    else {
      if (includeProperties && v.properties.contains(key))
        output_value = getProperty[T](key).get
      setState(key, output_value)
    }
    output_value
  }

  def appendToState[T: ClassTag](key: String, value: T): Unit = //write function later
    computationValues.get(key) match {
      case Some(arr) =>
        setState(key, arr.asInstanceOf[Array[T]] :+ value)
      case None      =>
        setState(key, Array(value))
    }

  //Send message
  override def messageSelf(data: Any): Unit =
    lens.sendMessage(VertexMessage(lens.superStep + 1, ID(), data))

  def messageVertex(vertexId: Long, data: Any): Unit = {
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
      case msg: VertexMessage[_, Long]             => multiQueue.receiveMessage(msg.superstep, msg.data)
      case msg: FilteredOutEdgeMessage[Long]       =>
        outgoingEdgeDeleteMultiQueue.receiveMessage(msg.superstep, msg.sourceId)
      case msg: FilteredInEdgeMessage[Long]        =>
        incomingEdgeDeleteMultiQueue.receiveMessage(msg.superstep, msg.sourceId)
      case msg: FilteredEdgeMessage[Long]          =>
        outgoingEdgeDeleteMultiQueue.receiveMessage(msg.superstep, msg.sourceId)
        incomingEdgeDeleteMultiQueue.receiveMessage(msg.superstep, msg.sourceId)
      case msg: GenericVertexMessage[(Long, Long)] => exploded(msg.vertexId._2).receiveMessage(msg)
    }

  def isFiltered = filtered

  def remove(): Unit = {
    // key is the vertex of the other side of edge
    filtered = true
    internalIncomingEdges.keys.foreach(k =>
      lens.sendMessage(FilteredOutEdgeMessage(lens.superStep + 1, k, ID()))
    )
    internalOutgoingEdges.keys.foreach(k =>
      lens.sendMessage(FilteredInEdgeMessage(lens.superStep + 1, k, ID()))
    )
  }

}
