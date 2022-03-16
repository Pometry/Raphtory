package com.raphtory.core.storage.pojograph.entities.external

import com.raphtory.core.components.querymanager.VertexMessage
import com.raphtory.core.graph.visitor.Edge
import com.raphtory.core.graph.visitor.ExplodedEdge
import com.raphtory.core.graph.visitor.Vertex
import com.raphtory.core.storage.pojograph.PojoGraphLens
import com.raphtory.core.storage.pojograph.entities.internal.PojoVertex
import com.raphtory.core.storage.pojograph.messaging.VertexMultiQueue
import io.prometheus.client.Collector
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap

import java.util.stream.Collectors
import scala.collection.mutable
import scala.reflect.ClassTag

/** @DoNotDocument */
class PojoExVertex(
    private val v: PojoVertex,
    private val internalIncomingEdges: Long2ObjectOpenHashMap[Edge],
    private val internalOutgoingEdges: Long2ObjectOpenHashMap[Edge],
    private val lens: PojoGraphLens
) extends PojoExEntity(v, lens)
        with Vertex {

  override def ID(): Long = v.vertexId

  private var multiQueue: VertexMultiQueue =
    new VertexMultiQueue() //Map of queues for all ongoing processing
  private var computationValues: Map[String, Any] =
    Map.empty //Partial results kept between supersteps in calculation

  def hasMessage(): Boolean =
    multiQueue.getMessageQueue(lens.superStep).nonEmpty

  def messageQueue[T]
      : List[T] = { //clears queue after getting it to make sure not there for next iteration
    val queue = multiQueue.getMessageQueue(lens.superStep).map(_.asInstanceOf[T])
    multiQueue.clearQueue(lens.superStep)
    queue
  }

  def clearMessageQueue(): Unit =
    multiQueue = new VertexMultiQueue()

  def voteToHalt(): Unit = lens.vertexVoted()

  //out edges whole
  def getOutEdges(after: Long = 0L, before: Long = Long.MaxValue): List[Edge] =
    allEdge(internalOutgoingEdges, after, before)

  //in edges whole
  def getInEdges(after: Long = 0L, before: Long = Long.MaxValue): List[Edge] =
    allEdge(internalIncomingEdges, after, before)

  //all edges
  def getEdges(after: Long = 0L, before: Long = Long.MaxValue): List[Edge] =
    getInEdges(after, before) ++ getOutEdges(after, before)

  //out edges individual
  def getOutEdge(id: Long, after: Long = 0L, before: Long = Long.MaxValue): Option[Edge] =
    individualEdge(internalOutgoingEdges, after, before, id)

  //In edges individual
  def getInEdge(id: Long, after: Long = 0L, before: Long = Long.MaxValue): Option[Edge] =
    individualEdge(internalIncomingEdges, after, before, id)

  // edge individual
  def getEdge(id: Long, after: Long = 0L, before: Long = Long.MaxValue): Option[Edge] = {
    val edges = internalIncomingEdges.clone
    edges.putAll(internalOutgoingEdges)
    individualEdge(edges, after, before, id)
  }

  override def explodeEdges(after: Long, before: Long): List[ExplodedEdge] =
    getEdges(after, before).flatMap(_.explode())

  override def explodeOutEdges(after: Long, before: Long): List[ExplodedEdge] =
    getOutEdges(after, before).flatMap(_.explode())

  override def explodeInEdges(after: Long, before: Long): List[ExplodedEdge] =
    getInEdges(after, before).flatMap(_.explode())

  override def explodeOutEdge(id: Long, after: Long, before: Long): Option[List[ExplodedEdge]] =
    getOutEdge(id, after, before) match {
      case Some(e) => Some(e.explode())
      case None    => None
    }

  override def explodeInEdge(id: Long, after: Long, before: Long): Option[List[ExplodedEdge]] =
    getInEdge(id, after, before) match {
      case Some(e) => Some(e.explode())
      case None    => None
    }

  private def allEdge(edges: Long2ObjectOpenHashMap[Edge], after: Long, before: Long) =
    if (after == 0 && before == Long.MaxValue)
      edges.values.stream.toArray.toList.asInstanceOf[List[Edge]]
    else
      edges.values.stream.filter(e => e.active(after, before)).toArray.toList.asInstanceOf[List[Edge]]//{case p: Edge => p})

  private def individualEdge(edges: Long2ObjectOpenHashMap[Edge], after: Long, before: Long, id: Long) =
    if (after == 0 && before == Long.MaxValue)
      Some(edges.get(id))
    else
      edges.containsKey(id) match {
        case true => if (edges.get(id).active(after, before)) Some(edges.get(id)) else None
        case false       => None
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

  def appendToState[T](key: String, value: Any): Unit = //write function later
    computationValues.get(key) match {
      case Some(arr) =>
        setState(key, arr.asInstanceOf[Array[Any]] :+ value)
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

  def messageOutNeighbours(message: Any): Unit =
    internalOutgoingEdges.keySet().forEach(vId => messageVertex(vId, message))

  def messageAllNeighbours(message: Any): Unit =
    internalOutgoingEdges.long2ObjectEntrySet().
      forEach(vId =>
        if (internalIncomingEdges.containsKey(vId.getLongKey))
          messageVertex(vId.getLongKey, message)
      )

  def messageInNeighbours(message: Any): Unit =
    internalIncomingEdges.long2ObjectEntrySet().
      forEach(vId =>
        if (internalIncomingEdges.containsKey(vId.getLongKey))
          messageVertex(vId.getLongKey, message)
      )

  // todo hide
  def receiveMessage[T](msg: VertexMessage[T]): Unit =
    multiQueue.receiveMessage(msg.superstep, msg.data)

  override def getOutNeighbours(after: Long, before: Long): List[Long] =
    getOutEdges(after, before).map(_.dst())

  override def getInNeighbours(after: Long, before: Long): List[Long] =
    getInEdges(after, before).map(_.src())

  override def getAllNeighbours(after: Long, before: Long): List[Long] =
    (getInNeighbours() ++ getOutNeighbours()).distinct

}
