package com.raphtory.core.implementations.chroniclegraph.entities.internal

import com.raphtory.core.implementations.chroniclegraph.external.{ChronicleExEdge, ChronicleExVertex}
import com.raphtory.core.implementations.pojograph.PojoGraphLens
import com.raphtory.core.model.graph.{GraphPartition, visitor}
import com.raphtory.core.model.graph.visitor.{Edge, Vertex}
import net.openhft.chronicle.map.ChronicleMap

import collection.JavaConverters._
import scala.collection.mutable

class ChronicleVertex(msgTime: Long, val vertexId: Long, initialValue: Boolean)
        extends ChronicleEntity(msgTime, initialValue) {

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


  def viewAtWithWindow(time: Long, windowSize: Long,lens:PojoGraphLens): Vertex = {
    val newInc = incomingEdges.keySet().asScala.collect {
      case key if incomingEdges.get(key).aliveAtWithWindow(time, windowSize) =>
        key.toLong -> new ChronicleExEdge(incomingEdges.get(key), key, lens)
    }.toSeq

    val newOut = outgoingEdges.keySet().asScala.collect {
      case key if outgoingEdges.get(key).aliveAtWithWindow(time, windowSize) =>
        key.toLong -> new ChronicleExEdge(outgoingEdges.get(key), key, lens)
    }.toSeq

    new ChronicleExVertex(this,
      mutable.Map(newInc:_*),
      mutable.Map(newOut:_*),
      lens)
  }

  override def createHistory(): ChronicleMap[Long, Boolean] = ???
}
