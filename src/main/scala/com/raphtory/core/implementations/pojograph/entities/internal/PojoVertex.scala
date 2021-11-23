package com.raphtory.core.implementations.pojograph.entities.internal


import com.raphtory.core.implementations.pojograph.PojoGraphLens
import com.raphtory.core.implementations.pojograph.entities.external.{PojoExEdge, PojoExVertex}
import com.raphtory.core.model.graph.{GraphPartition, visitor}
import com.raphtory.core.model.graph.visitor.{Edge, Vertex}

import scala.collection.mutable

class PojoVertex(msgTime: Long, val vertexId: Long, initialValue: Boolean)
        extends PojoEntity(msgTime, initialValue) {

  var incomingEdges = mutable.Map[Long, PojoEdge]() //Map of all edges associated with the vertex
  var outgoingEdges = mutable.Map[Long, PojoEdge]()

  private var edgesRequiringSync = 0

  //Functions for adding associated edges to this vertex
  def incrementEdgesRequiringSync()  =edgesRequiringSync+=1
  def getEdgesRequringSync() = edgesRequiringSync
  def addIncomingEdge(edge: PojoEdge): Unit = incomingEdges.put(edge.getSrcId, edge)
  def addOutgoingEdge(edge: PojoEdge): Unit = outgoingEdges.put(edge.getDstId, edge)
  def addAssociatedEdge(edge: PojoEdge): Unit =
    if (edge.getSrcId == vertexId) addOutgoingEdge(edge) else addIncomingEdge(edge)
  def getOutgoingEdge(id: Long): Option[PojoEdge] = outgoingEdges.get(id)
  def getIncomingEdge(id: Long): Option[PojoEdge] = incomingEdges.get(id)


  def viewAtWithWindow(time: Long, windowSize: Long,lens:PojoGraphLens): Vertex = {
    new PojoExVertex(this,
      incomingEdges.collect {
        case (k, edge) if edge.aliveAtWithWindow(time,windowSize) =>
          k -> new PojoExEdge(edge, k, lens)
      },
      outgoingEdges.collect {
        case (k, edge) if edge.aliveAtWithWindow(time,windowSize) =>
          k -> new PojoExEdge(edge, k, lens)
      },
      lens)
  }

  override def dedupe() = {
    super.dedupe()
    incomingEdges.foreach(_._2.dedupe())
    outgoingEdges.foreach(_._2.dedupe())
  }

  //def serialise(): ParquetVertex = ParquetVertex(vertexId,history.toList,properties.map(x=> x._2.serialise(x._1)).toList,incomingEdges.map(x=>x._2.serialise()).toList,outgoingEdges.map(x=>x._2.serialise()).toList)


}
