package com.raphtory.core.actors.partitionmanager.Archivist.Helpers

import akka.actor.{Actor, ActorRef, Props}
import com.raphtory.core.model.communication._
import com.raphtory.core.model.graphentities.{Edge, Entity, Vertex}
import com.raphtory.core.storage.EntityStorage
import com.raphtory.core.utils.KeyEnum
import monix.eval.Task

import scala.collection.parallel.mutable.ParTrieMap
import scala.util.{Failure, Success}

class ArchivingSlave extends Actor{

  var childMap = ParTrieMap[Int,ActorRef]()
  val compressing    : Boolean =  System.getenv().getOrDefault("COMPRESSING", "true").trim.toBoolean
  val saving    : Boolean =  System.getenv().getOrDefault("SAVING", "true").trim.toBoolean

  var startedArchiving = 0
  var finishedArchiving = 0
  var percentcheck = 1
  var percenting = false

  var propsRemoved =  0
  var historyRemoved =  0
  var verticesRemoved =  0
  var edgesRemoved =  0

  override def receive:Receive = {
    case SetupSlave(children) => setup(children)
    case ArchiveEdges(ls) => {archiveEdges(ls)}
    case ArchiveVertices(ls) => {archiveVertices(ls)}
    case FinishedEdgeArchiving(key,archived) => finishedEdge(key,archived)
    case FinishedVertexArchiving(key,archived) => finishedVertex(key,archived)

    case ArchiveEdge(key,time) => archiveEdge(key,time)
    case ArchiveVertex(key,time) => archiveVertex(key,time)

  }

  def setup(children: Int) = {
    for(i <- 0 to children){
      childMap.put(i,context.actorOf(Props[ArchivingSlave],s"child_$i"))
    }
  }


  def archiveEdges(removalPoint:Long) = {
    val size = childMap.size
    finishedArchiving=0
    startedArchiving=0
    EntityStorage.edges.keySet.foreach(key => {
      startedArchiving +=1
      childMap.getOrElse(startedArchiving%size,null) ! ArchiveEdge(key,removalPoint)
    })
    println(s"starting value edges for archiving at ${System.currentTimeMillis()/1000}: $startedArchiving")
    percentcheck = startedArchiving/10
  }

  def archiveVertices(removalPoint:Long) = {
    val size = childMap.size
    finishedArchiving=0
    startedArchiving=0
    EntityStorage.vertices.keySet.foreach(key =>{
      startedArchiving +=1
      childMap.getOrElse(startedArchiving % size, null) ! ArchiveVertex(key, removalPoint)
    })
    println(s"starting value edges for archiving at ${System.currentTimeMillis()/1000}: $startedArchiving")
    percentcheck = startedArchiving/10
  }

  def finishedEdge(key: Long,archived: (Int,Int,Int)) = {
    finishedArchiving +=1
    propsRemoved      += archived._1
    historyRemoved    += archived._2
    edgesRemoved      += archived._3
    //if((finishedArchiving%percentcheck==0) && startedArchiving>0 && percenting)
    //    println(s"Edge Archiving ${(finishedArchiving * 100) / startedArchiving;}% Complete")
    if(startedArchiving==finishedArchiving) {
      context.parent ! FinishedEdgeArchiving(finishedArchiving, (propsRemoved, historyRemoved, edgesRemoved))
      propsRemoved = 0
      historyRemoved = 0
      edgesRemoved = 0
    }
  }

  def finishedVertex(key:Int,archived: (Int,Int,Int))={
    finishedArchiving +=1
    propsRemoved      += archived._1
    historyRemoved    += archived._2
    verticesRemoved   += archived._3
//    if(finishedArchiving%percentcheck==0 && startedArchiving>0 && percenting)
 //       println(s"Vertex Archiving ${(finishedArchiving * 100) / startedArchiving;}% Complete")
    if(startedArchiving==finishedArchiving){
      context.parent ! FinishedVertexArchiving(finishedArchiving,(propsRemoved,historyRemoved,verticesRemoved))
      propsRemoved = 0
      historyRemoved = 0
      verticesRemoved = 0
    }
  }


  //SLAVE SLAVE
  def archiveEdge(key:Long,now:Long) = {
    EntityStorage.edges.synchronized {
      EntityStorage.edges.get(key) match {
        case Some(edge) => context.parent ! FinishedEdgeArchiving(key, edgeMaximumHistory(edge, now))
        case None => //do nothing
      }
    }
  }
  def archiveVertex(key:Int,now:Long) = {
    EntityStorage.vertices.synchronized {
      EntityStorage.vertices.get(key) match {
        case Some(vertex) => context.parent ! FinishedVertexArchiving(key,vertexMaximumHistory(vertex,now))
        case None => //do nothing
      }
    }
  }

  def vertexMaximumHistory(e:Vertex,removalPoint:Long):(Int,Int,Int) = {
    val (placeholder, allOld,removed,propremoved) = e.removeAncientHistory(removalPoint,compressing)
    if (placeholder.asInstanceOf[Boolean]) {/*TODO decide what to do with placeholders (future)*/}
    var total = 0
    if (allOld.asInstanceOf[Boolean]) {
      EntityStorage.vertices.remove(e.getId.toInt)
      total +=1
    }
    (propremoved.asInstanceOf[Int],removed.asInstanceOf[Int],total)
  }

  def edgeMaximumHistory(e:Edge,removalPoint:Long):(Int,Int,Int) = {
    val (placeholder, allOld,removed,propremoved) = e.removeAncientHistory(removalPoint,compressing)
    if (placeholder.asInstanceOf[Boolean]) {/*TODO decide what to do with placeholders (future)*/}
    var total = 0
    if (allOld.asInstanceOf[Boolean]) {
      EntityStorage.edges.remove(e.getId)
      total +=1
    }
    (propremoved.asInstanceOf[Int],removed.asInstanceOf[Int],total)
  }

}
