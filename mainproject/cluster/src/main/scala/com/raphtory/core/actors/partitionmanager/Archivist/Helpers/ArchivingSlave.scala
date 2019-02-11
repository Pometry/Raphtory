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
  var removalPoint = 0l
  var archivePercentage = 30f
  val compressing    : Boolean =  System.getenv().getOrDefault("COMPRESSING", "true").trim.toBoolean
  val saving    : Boolean =  System.getenv().getOrDefault("SAVING", "true").trim.toBoolean

  var startedArchiving = 0
  var finishedArchiving = 0
  var percentcheck = 0

  val propsRemoved =  0
  val historyRemoved =  0
  val verticesRemoved =  0
  val edgesRemoved =  0

  override def receive:Receive = {
    case SetupSlave(children) => setup(children)
    case ArchiveEdges(ls) => {removalPoint = ls;archiveEdges()}
    case ArchiveVertices(ls) => {removalPoint=ls;archiveVertices()}
    case FinishedEdgeArchiving(key) => finishedEdge(key)
    case FinishedVertexArchiving(key) => finishedVertex(key)

    case ArchiveEdge(key,time) => compressEdge(key,time)
    case ArchiveVertex(key,time) => compressVertex(key,time)

  }

  def setup(children: Int) = {
    for(i <- 0 to children){
      childMap.put(i,context.actorOf(Props[ArchivingSlave],s"child_$i"))
    }
  }


  def archiveEdges() = {
    val size = childMap.size
    finishedArchiving=0
    startedArchiving=0
    EntityStorage.edges.keySet.foreach(key => {
      startedArchiving +=1
      EntityStorage.edges.synchronized {
        startedArchiving +=1
        childMap.getOrElse(startedArchiving%size,null) ! ArchiveEdge(key,now)
      }
    })
  }

  def archiveVertices() = {
    val size = childMap.size
    finishedArchiving=0
    startedArchiving=0
    EntityStorage.vertices.keySet.foreach(key =>{
      startedArchiving +=1
      EntityStorage.vertices.synchronized {
        startedArchiving += 1
        childMap.getOrElse(startedArchiving % size, null) ! ArchiveVertex(key, now)
      }
    })
  }

  def finishedEdge(key: Long) = {
    finishedArchiving +=1
    if(finishedArchiving%percentcheck==0){
      if(finishedArchiving>0)
        println(s"Edge compression ${(finishedArchiving * 100) / startedArchiving;}% Complete")
    }
    if(startedArchiving==finishedArchiving) {
      finishedArchiving=0
      startedArchiving=0
      context.parent ! FinishedEdgeArchiving(finishedArchiving)

    }
  }

  def finishedVertex(key:Int)={
    finishedArchiving +=1
    if(finishedArchiving%percentcheck==0){
      if(startedArchiving>0)
        println(s"Vertex compression ${(finishedArchiving * 100) / startedArchiving;}% Complete")
    }
    if(startedArchiving==finishedArchiving) {
      finishedArchiving=0
      startedArchiving=0
      context.parent ! FinishedVertexArchiving(finishedArchiving)

    }
  }


  //SLAVE SLAVE

  def compressEdge(key:Long,now:Long) = {
    EntityStorage.edges.synchronized {
      EntityStorage.edges.get(key) match {
        case Some(edge) => checkMaximumHistory(edge, KeyEnum.edges, removalPoint)
        case None => //do nothing
      }
    }
    context.parent ! FinishedEdgeArchiving(key)
  }

  def compressVertex(key:Int,now:Long) = {
    EntityStorage.vertices.synchronized {
      EntityStorage.vertices.get(key) match {
        case Some(vertex) => checkMaximumHistory(vertex, KeyEnum.vertices,removalPoint)
        case None => //do nothing
      }
    }
    context.parent ! FinishedVertexArchiving(key)
  }


  def checkMaximumHistory(e:Entity, et : KeyEnum.Value,removalPoint:Long) = {
    val (placeholder, allOld,removed,propremoved) = e.removeAncientHistory(removalPoint,compressing)
    if (placeholder.asInstanceOf[Boolean]) {/*TODO decide what to do with placeholders (future)*/}
    propsRemoved += propremoved.asInstanceOf[Int]
    historyRemoved += removed.asInstanceOf[Int]
    if (allOld.asInstanceOf[Boolean]) {
      et match {
        case KeyEnum.vertices => {
          EntityStorage.vertices.remove(e.getId.toInt)
          verticesRemoved.add(1)
        }
        case KeyEnum.edges    => {
          EntityStorage.edges.remove(e.getId)
          edgesRemoved.add(1)
        }
      }
    }
  }

}
