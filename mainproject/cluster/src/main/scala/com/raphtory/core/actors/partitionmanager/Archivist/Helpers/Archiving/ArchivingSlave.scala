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
   // case ArchiveEdge(key,time) => archiveEdge(key,time)
   // case ArchiveVertex(key,time) => archiveVertex(key,time)

  }

  def archiveEdges(now:Long) = {
    var edgesRemoved = 0
    var propsRemoved = 0
    var historyRemoved = 0
    EntityStorage.edgeKeys.get(id) match {
      case Some(set) => set.foreach(key => {
        val removed = archiveEdge(key,now)
        propsRemoved += removed._1
        historyRemoved += removed._2
        edgesRemoved += removed._3
      })
    }
    context.parent ! FinishedEdgeArchiving(id,(propsRemoved,historyRemoved,edgesRemoved))
  }

  def archiveVertices(now:Long) = {
    var verticesRemoved = 0
    var propsRemoved = 0
    var historyRemoved = 0
    EntityStorage.vertexKeys.get(id) match {
      case Some(set) => set.foreach(key => {
        val removed = archiveVertex(key,now,set)
        propsRemoved += removed._1
        historyRemoved += removed._2
        verticesRemoved += removed._3
      })
    }
    context.parent ! FinishedVertexArchiving(id,(propsRemoved,historyRemoved,verticesRemoved))
  }


  def archiveEdge(key:Long,now:Long):(Int,Int,Int) = {
    //EntityStorage.edges.synchronized {
      EntityStorage.edges.get(key) match {
        case Some(edge) =>  edgeMaximumHistory(edge, now)
        case None => (0,0,0)//do nothing
      }
    //}
  }
  def archiveVertex(key:Int,now:Long,set:ParSet[Int]) = {
    //EntityStorage.vertices.synchronized {
      EntityStorage.vertices.get(key) match {
        case Some(vertex) => vertexMaximumHistory(vertex,now,set)
        case None => (0,0,0)//do nothing
      }
    //}
  }

  def vertexMaximumHistory(e:Vertex,removalPoint:Long,set:ParSet[Int]):(Int,Int,Int) = {
    val (placeholder, allOld,removed,propremoved) = e.removeAncientHistory(removalPoint,compressing)
    if (placeholder.asInstanceOf[Boolean]) {/*TODO decide what to do with placeholders (future)*/}
    var total = 0
    if (allOld.asInstanceOf[Boolean]) {
      EntityStorage.vertices.synchronized {
        EntityStorage.vertices.remove(e.getId.toInt)
      }
      total +=1
    }
    (propremoved.asInstanceOf[Int],removed.asInstanceOf[Int],total)
  }

  def edgeMaximumHistory(e:Edge,removalPoint:Long):(Int,Int,Int) = {
    val (placeholder, allOld,removed,propremoved) = e.removeAncientHistory(removalPoint,compressing)
    if (placeholder.asInstanceOf[Boolean]) {/*TODO decide what to do with placeholders (future)*/}
    var total = 0
    if (allOld.asInstanceOf[Boolean]) {
      EntityStorage.edges.synchronized {
        EntityStorage.edges.remove(e.getId)
      }
      total += 1
    }
    (propremoved.asInstanceOf[Int],removed.asInstanceOf[Int],total)
  }



}
