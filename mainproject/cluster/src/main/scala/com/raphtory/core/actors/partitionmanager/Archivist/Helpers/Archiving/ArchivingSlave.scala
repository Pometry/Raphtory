package com.raphtory.core.actors.partitionmanager.Archivist.Helpers.Archiving

import akka.actor.Actor
import com.raphtory.core.model.communication._
import com.raphtory.core.model.graphentities.{Edge, Vertex}
import com.raphtory.core.storage.EntityStorage

import scala.collection.parallel.mutable.ParSet

class ArchivingSlave(id:Int) extends Actor{

  val compressing    : Boolean =  System.getenv().getOrDefault("COMPRESSING", "true").trim.toBoolean
  val saving    : Boolean =  System.getenv().getOrDefault("SAVING", "true").trim.toBoolean

  override def receive:Receive = {
    case ArchiveEdges(ls) => archiveEdges(ls)
    case ArchiveVertices(ls) => archiveVertices(ls)
  }

  def archiveEdges(now:Long) = {
    EntityStorage.edgeKeys.get(id) match {
      case Some(set) => set.foreach(key => {archiveEdge(key,now)})
    }
    context.parent ! FinishedEdgeArchiving(id)
  }

  def archiveVertices(now:Long) = {
    EntityStorage.vertexKeys.get(id) match {
      case Some(set) => set.foreach(key => {
        archiveVertex(key,now)
      })
    }
    context.parent ! FinishedVertexArchiving(id)
  }

  //TODO decide what to do with placeholders (future)*
  def archiveEdge(key:Long, cutoff:Long) = {
      EntityStorage.edges.get(key) match {
        case Some(edge) => if (edge.archive(cutoff,compressing)) EntityStorage.edges.remove(e.getId)//if all old then remove the edge
        case None => {}//do nothing
      }
  }
  def archiveVertex(key:Int,cutoff:Long) = {
      EntityStorage.vertices.get(key) match {
        case Some(vertex) => if (vertex.archive(cutoff,compressing)) EntityStorage.vertices.remove(vertex.getId.toInt) //if all old then remove the vertex
        case None => {}//do nothing
      }
  }
}
