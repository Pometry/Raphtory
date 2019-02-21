package com.raphtory.core.actors.partitionmanager.Archivist.Helpers

import akka.actor.Actor
import com.raphtory.core.model.communication._
import com.raphtory.core.model.graphentities.{Edge, Vertex}
import com.raphtory.core.storage.EntityStorage

class CompressionSlave extends Actor {
  val compressing: Boolean = System.getenv().getOrDefault("COMPRESSING", "true").trim.toBoolean
  val saving: Boolean = System.getenv().getOrDefault("SAVING", "true").trim.toBoolean

  override def receive: Receive = {
    case CompressVertex(key, time) => compressVertex(key, time)
    case CompressEdge(key, time) => compressEdge(key, time)

  }

  //SLAVE SLAVE

  def compressEdge(key: Long, now: Long) = {
    EntityStorage.edges.synchronized {
      EntityStorage.edges.get(key) match {
        case Some(edge) => saveEdge(edge, now)
        case None => //do nothing
      }
    }
    context.parent ! FinishedEdgeCompression(key)
  }

  def compressVertex(key: Int, now: Long) = {
    EntityStorage.vertices.synchronized {
      EntityStorage.vertices.get(key) match {
        case Some(vertex) => saveVertex(vertex, now)
        case None => //do nothing
      }
    }


    context.parent ! FinishedVertexCompression(key)
  }


  def saveEdge(edge: Edge, cutOff: Long) = {
    val history = edge.compressAndReturnOldHistory(cutOff)
    if (saving) {
      if (history.size > 0) {
        //RaphtoryDBWrite.edgeHistory.save(edge.getSrcId, edge.getDstId, history)
      }
      edge.properties.foreach(property => {
        val propHistory = property._2.compressAndReturnOldHistory(cutOff)
        if (propHistory.size > 0) {
          //RaphtoryDBWrite.edgePropertyHistory.save(edge.getSrcId, edge.getDstId, property._1, propHistory)
        }
      })
    }
  }

  def saveVertex(vertex: Vertex, cutOff: Long) = {
    val history = vertex.compressAndReturnOldHistory(cutOff)
    if (saving) { //if we are saving data to cassandra
      if (history.size > 0) {
        //RaphtoryDBWrite.vertexHistory.save(vertex.getId, history)
      }
      vertex.properties.foreach(prop => {
        val propHistory = prop._2.compressAndReturnOldHistory(cutOff)
        if (propHistory.size > 0) {
          //RaphtoryDBWrite.vertexPropertyHistory.save(vertex.getId, prop._1, propHistory)
        }
      })
    }
  }
}
