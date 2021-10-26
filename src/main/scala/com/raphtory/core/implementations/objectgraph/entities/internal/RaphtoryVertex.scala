package com.raphtory.core.implementations.objectgraph.entities.internal

import com.raphtory.core.implementations.objectgraph.ObjectGraphLens
import com.raphtory.core.implementations.objectgraph.entities.external.{ObjectEdge, ObjectVertex}
import com.raphtory.core.model.graph.{GraphPartition, visitor}
import com.raphtory.core.model.graph.visitor.{Edge, Vertex}

import scala.collection.mutable

/** Companion Vertex object (extended creator for storage loads) */
object RaphtoryVertex {
  def apply(
      creationTime: Long,
      vertexId: Int,
      previousState: mutable.TreeMap[Long, Boolean],
      properties: mutable.Map[String, Property],
      storage: GraphPartition
  ) = {
    val v = new RaphtoryVertex(creationTime, vertexId, initialValue = true)
    v.history = previousState
    //v.associatedEdges = associatedEdges
    v.properties = properties
    v
  }

//  def apply(parquet: ParquetVertex):RaphtoryVertex = {
//    val vertex = new RaphtoryVertex(parquet.history.head._1,parquet.id,parquet.history.head._2)
//    parquet.history.foreach(update=> if(update._2) vertex.revive(update._1) else vertex.kill(update._1))
//    parquet.properties.foreach(prop=> vertex.properties +=((prop.key,Property(prop))))
//    parquet.incoming.foreach(edge=> vertex.incomingEdges+=((edge.src,RaphtoryEdge(edge))))
//    parquet.outgoing.foreach(edge=> vertex.outgoingEdges+=((edge.dst,RaphtoryEdge(edge))))
//    vertex
//  }

}

class RaphtoryVertex(msgTime: Long, val vertexId: Long, initialValue: Boolean)
        extends RaphtoryEntity(msgTime, initialValue) {

  var incomingEdges = mutable.Map[Long, RaphtoryEdge]() //Map of all edges associated with the vertex
  var outgoingEdges = mutable.Map[Long, RaphtoryEdge]()

  private var edgesRequiringSync = 0

  //Functions for adding associated edges to this vertex
  def incrementEdgesRequiringSync()  =edgesRequiringSync+=1
  def getEdgesRequringSync() = edgesRequiringSync
  def addIncomingEdge(edge: RaphtoryEdge): Unit = incomingEdges.put(edge.getSrcId, edge)
  def addOutgoingEdge(edge: RaphtoryEdge): Unit = outgoingEdges.put(edge.getDstId, edge)
  def addAssociatedEdge(edge: RaphtoryEdge): Unit =
    if (edge.getSrcId == vertexId) addOutgoingEdge(edge) else addIncomingEdge(edge)
  def getOutgoingEdge(id: Long): Option[RaphtoryEdge] = outgoingEdges.get(id)
  def getIncomingEdge(id: Long): Option[RaphtoryEdge] = incomingEdges.get(id)


  def viewAtWithWindow(time: Long, windowSize: Long,lens:ObjectGraphLens): Vertex = {
    new ObjectVertex(this,
      incomingEdges.collect {
        case (k, edge) if edge.aliveAtWithWindow(time,windowSize) =>
          k -> new ObjectEdge(edge, k, lens)
      },
      outgoingEdges.collect {
        case (k, edge) if edge.aliveAtWithWindow(time,windowSize) =>
          k -> new ObjectEdge(edge, k, lens)
      },
      lens)
  }

  //def serialise(): ParquetVertex = ParquetVertex(vertexId,history.toList,properties.map(x=> x._2.serialise(x._1)).toList,incomingEdges.map(x=>x._2.serialise()).toList,outgoingEdges.map(x=>x._2.serialise()).toList)


}
