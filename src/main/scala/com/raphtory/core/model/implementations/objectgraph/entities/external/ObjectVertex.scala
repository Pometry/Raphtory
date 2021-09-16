package com.raphtory.core.model.implementations.objectgraph.entities.external

import com.raphtory.core.analysis.ObjectGraphLens
import com.raphtory.core.model.communication.{VertexMessage, VertexMultiQueue}
import com.raphtory.core.model.graph.visitor.{Edge, EntityVisitor, Vertex}
import com.raphtory.core.model.implementations.objectgraph.entities.internal.RaphtoryVertex

import scala.collection.parallel.mutable.ParTrieMap
import scala.reflect.ClassTag

class ObjectVertex(private val v: RaphtoryVertex,
  private val internalIncomingEdges: ParTrieMap[Long, Edge],
  private val internalOutgoingEdges: ParTrieMap[Long, Edge],
  private val lens: ObjectGraphLens
  ) extends ObjectEntity(v,lens) with Vertex {

  override def ID() = v.vertexId

  private val multiQueue: VertexMultiQueue = new VertexMultiQueue() //Map of queues for all ongoing processing
  private var computationValues: Map[String, Any] = Map.empty //Partial results kept between supersteps in calculation

  def hasMessage(): Boolean = multiQueue.getMessageQueue(lens.superStep).nonEmpty

  def messageQueue[T: ClassTag]: List[T] = { //clears queue after getting it to make sure not there for next iteration
    val queue = multiQueue.getMessageQueue(lens.superStep).map(_.asInstanceOf[T])
    multiQueue.clearQueue(lens.superStep)
    queue
  }

  def voteToHalt(): Unit = lens.vertexVoted()

  //out edges whole
  def getOutEdges(after:Long=0L,before:Long=Long.MaxValue): List[Edge] = allEdge(internalOutgoingEdges,after,before)

  //in edges whole
  def getInEdges(after:Long=0L,before:Long=Long.MaxValue): List[Edge] = allEdge(internalIncomingEdges,after,before)

      //all edges
  def getEdges(after:Long=0L,before:Long=Long.MaxValue): List[Edge] = getInEdges(after,before) ++ getOutEdges(after,before)

  //out edges individual
  def getOutEdge(id: Long,after:Long=0L,before:Long=Long.MaxValue): Option[Edge] = individualEdge(internalOutgoingEdges,after,before,id)
  //In edges individual
  def getInEdge(id: Long,after:Long=0L,before:Long=Long.MaxValue): Option[Edge]  = individualEdge(internalIncomingEdges,after,before,id)


  private def allEdge(edges:ParTrieMap[Long, Edge],after:Long,before:Long) = {
    if(after==0&&before==Long.MaxValue)
      edges.map(x => x._2).toList
    else
      edges.collect {
        case (id, edge) if edge.active(after,before) => edge
      }.toList
  }

  private def individualEdge(edges:ParTrieMap[Long, Edge],after:Long,before:Long,id:Long) = {
    if(after==0&&before==Long.MaxValue)
      edges.get(id)
    else  edges.get(id) match {
      case Some(edge) => if(edge.active(after,before)) Some(edge) else None
      case None => None
    }
  }


  // state related
  def setState(key: String, value: Any): Unit =
    computationValues += ((key, value))

  def getState[T: ClassTag](key: String) =
    computationValues(key).asInstanceOf[T]

  def containsState(key: String): Boolean =
    computationValues.contains(key)

  def getOrSetState[T: ClassTag](key: String, value: T): T =
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