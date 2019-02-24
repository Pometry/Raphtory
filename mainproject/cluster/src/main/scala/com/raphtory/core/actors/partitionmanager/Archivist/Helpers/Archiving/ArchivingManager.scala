package com.raphtory.core.actors.partitionmanager.Archivist.Helpers.Archiving

import akka.actor.{Actor, ActorRef, Props}
import com.raphtory.core.model.communication._
import com.raphtory.core.model.graphentities.{Edge, Vertex}
import com.raphtory.core.storage.EntityStorage

import scala.collection.parallel.mutable.ParTrieMap

class ArchivingManager extends Actor{

  var childMap = ParTrieMap[Int,ActorRef]()
  var startedArchiving = 0
  var finishedArchiving = 0

  override def receive:Receive = {
    case SetupSlave(children) => setup(children)
    case ArchiveEdges(ls) => {archiveEdges(ls)}
    case ArchiveVertices(ls) => {archiveVertices(ls)}
    case FinishedEdgeArchiving(key,archived) => finishedEdge(key,archived)
    case FinishedVertexArchiving(key,archived) => finishedVertex(key,archived)

  }

  def setup(children: Int) = {
    for(i <- 0 until children){
      childMap.put(i,context.actorOf(Props(new ArchivingSlave(i)),s"child_$i"))
    }
    println(s"number of children = $children ${childMap.size}")
  }

  def archiveEdges(removalPoint:Long) = {
    startedArchiving = childMap.size
    childMap.values.foreach(child => child ! ArchiveEdges(removalPoint))
    println(s"starting value edges for archiving at ${System.currentTimeMillis()/1000}: $startedArchiving")
  }

  def archiveVertices(removalPoint:Long) = {
    startedArchiving= childMap.size
    childMap.values.foreach(child => child ! ArchiveVertices(removalPoint))
    println(s"starting value vertices for archiving at ${System.currentTimeMillis()/1000}: $startedArchiving")
  }

  def finishedEdge(ID: Long,archived: (Int,Int,Int)) = {
    finishedArchiving +=1
    if(startedArchiving==finishedArchiving) {
      context.parent ! FinishedEdgeArchiving(finishedArchiving, archived)
      finishedArchiving = 0
    }
  }

  def finishedVertex(ID:Int,archived: (Int,Int,Int))={
    finishedArchiving +=1
    if(startedArchiving==finishedArchiving){
      context.parent ! FinishedVertexArchiving(finishedArchiving,archived)
      finishedArchiving = 0
    }
  }



}
