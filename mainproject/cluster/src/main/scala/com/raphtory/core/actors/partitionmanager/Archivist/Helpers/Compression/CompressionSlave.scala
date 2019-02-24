package com.raphtory.core.actors.partitionmanager.Archivist.Helpers.Compression

import akka.actor.Actor
import com.raphtory.core.model.communication._
import com.raphtory.core.model.graphentities.{Edge, Vertex}
import com.raphtory.core.storage.{EntityStorage, RaphtoryDBWrite}

class CompressionSlave(id:Int) extends Actor {
  val compressing: Boolean = System.getenv().getOrDefault("COMPRESSING", "true").trim.toBoolean
  val saving: Boolean = System.getenv().getOrDefault("SAVING", "true").trim.toBoolean

  override def receive: Receive = {
    case CompressEdges(ls) => {compressEdges(ls)}
    case CompressVertices(ls) => {compressVertices(ls)}
  }

  //SLAVE SLAVE

  def compressEdges(now:Long) = {
    EntityStorage.edgeKeys.get(id) match {
      case Some(set) => set.foreach(key => compressEdge(key,now))
    }
    context.parent ! FinishedEdgeCompression(id)
  }

  def compressVertices(now:Long) = {
    EntityStorage.vertexKeys.get(id) match {
      case Some(set) => set.foreach(key => compressVertex(key,now))
    }
    context.parent ! FinishedVertexCompression(id)
  }

  def compressEdge(key: Long, now: Long) = {
    EntityStorage.edges.synchronized {
      EntityStorage.edges.get(key) match {
        case Some(edge) => saveEdge(edge, now)
        case None => //do nothing
      }
    }

  }

  def compressVertex(key: Int, now: Long) = {
    EntityStorage.vertices.synchronized {
      EntityStorage.vertices.get(key) match {
        case Some(vertex) => saveVertex(vertex, now)
        case None => //do nothing
      }
    }
  }


  def saveEdge(edge: Edge, cutOff: Long) = {
    val history = edge.compressAndReturnOldHistory(cutOff)
    if (saving) {
      if (history.size > 0) {
        RaphtoryDBWrite.edgeHistory.save(edge.getSrcId, edge.getDstId, history)
      }
      edge.properties.foreach(property => {
        val propHistory = property._2.compressAndReturnOldHistory(cutOff)
        if (propHistory.size > 0) {
          RaphtoryDBWrite.edgePropertyHistory.save(edge.getSrcId, edge.getDstId, property._1, propHistory)
        }
      })
    }
  }

  def saveVertex(vertex: Vertex, cutOff: Long) = {
    val history = vertex.compressAndReturnOldHistory(cutOff)
    if (saving) { //if we are saving data to cassandra
      if (history.size > 0) {
        RaphtoryDBWrite.vertexHistory.save(vertex.getId, history)
      }
      vertex.properties.foreach(prop => {
        val propHistory = prop._2.compressAndReturnOldHistory(cutOff)
        if (propHistory.size > 0) {
          RaphtoryDBWrite.vertexPropertyHistory.save(vertex.getId, prop._1, propHistory)
        }
      })
    }
  }
}
