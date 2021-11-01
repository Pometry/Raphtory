package com.raphtory.core.implementations.chroniclegraph.entities.internal

import com.raphtory.core.implementations.generic.GenericGraphLens
import com.raphtory.core.implementations.generic.entity.external.{GenericEdge, GenericVertex}
import com.raphtory.core.implementations.generic.entity.internal.{InternalProperty, InternalVertex}
import com.raphtory.core.model.graph.{GraphPartition, visitor}
import com.raphtory.core.model.graph.visitor.{Edge, Vertex}
import net.openhft.chronicle.map.ChronicleMap

import collection.JavaConverters._
import scala.collection.mutable

/** Companion Vertex object (extended creator for storage loads) */
object ChronicleVertex {
  def apply(
             creationTime: Long,
             vertexId: Int,
             previousState: mutable.TreeMap[Long, Boolean],
             properties: mutable.Map[String, InternalProperty],
             storage: GraphPartition
  ) = {
    val v = new ChronicleVertex(creationTime, vertexId, initialValue = true)
    v.history = previousState
    //v.associatedEdges = associatedEdges
    v.properties = properties
    v
  }

}

class ChronicleVertex(msgTime: Long, val vertexId: Long, initialValue: Boolean)
        extends ChronicleEntity(msgTime, initialValue) with InternalVertex {

  val incomingEdges = ChronicleMap.of(classOf[java.lang.Long], classOf[ChronicleEdge])
    .entries(500)
    .averageValueSize(1000)
    .name(vertexId+"in")
    .create()

  val outgoingEdges = ChronicleMap.of(classOf[java.lang.Long], classOf[ChronicleEdge])
    .entries(500)
    .averageValueSize(1000)
    .name(vertexId+"out")
    .create()

  private var edgesRequiringSync = 0

  //Functions for adding associated edges to this vertex
  def incrementEdgesRequiringSync()  =edgesRequiringSync+=1
  def getEdgesRequringSync() = edgesRequiringSync
  def addIncomingEdge(edge: ChronicleEdge): Unit = incomingEdges.put(edge.getSrcId, edge)
  def addOutgoingEdge(edge: ChronicleEdge): Unit = outgoingEdges.put(edge.getDstId, edge)
  def addAssociatedEdge(edge: ChronicleEdge): Unit =
    if (edge.getSrcId == vertexId) addOutgoingEdge(edge) else addIncomingEdge(edge)
  def getOutgoingEdge(id: Long): Option[ChronicleEdge] = Option(outgoingEdges.get(id))
  def getIncomingEdge(id: Long): Option[ChronicleEdge] = Option(incomingEdges.get(id))


  def viewAtWithWindow(time: Long, windowSize: Long,lens:GenericGraphLens): Vertex = {
    val newInc = incomingEdges.keySet().asScala.collect {
      case key if incomingEdges.get(key).aliveAtWithWindow(time, windowSize) =>
        key.toLong -> new GenericEdge(incomingEdges.get(key), key, lens)
    }.toSeq

    val newOut = outgoingEdges.keySet().asScala.collect {
      case key if outgoingEdges.get(key).aliveAtWithWindow(time, windowSize) =>
        key.toLong -> new GenericEdge(outgoingEdges.get(key), key, lens)
    }.toSeq

    new GenericVertex(this,
      mutable.Map(newInc:_*),
      mutable.Map(newOut:_*),
      lens)
  }

}
