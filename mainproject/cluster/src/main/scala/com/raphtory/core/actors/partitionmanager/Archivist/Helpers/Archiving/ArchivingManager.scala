package com.raphtory.core.actors.partitionmanager.Archivist.Helpers.Archiving

import akka.actor.{Actor, ActorRef, Props}
import com.raphtory.core.model.communication._
import com.raphtory.core.model.graphentities.{Edge, Vertex}
import com.raphtory.core.storage.EntityStorage

import scala.collection.parallel.mutable.ParTrieMap

class ArchivingManager extends Actor{

  var childMap = ParTrieMap[Int,ActorRef]()
  var startedArchiving = 10
  var finishedArchiving = 0

  override def receive:Receive = {
    case SetupSlave(children) => setup(children)
    case ArchiveEdges(ls) => {archiveEdges(ls)}
    case ArchiveVertices(ls) => {archiveVertices(ls)}
    case FinishedEdgeArchiving(key) => finishedEdge(key)
    case FinishedVertexArchiving(key) => finishedVertex(key)

  }

  def setup(children: Int) = {
    for(i <- 0 until children){
      childMap.put(i,context.actorOf(Props(new ArchivingSlave(i)),s"child_$i"))
    }
    println(s"number of children = $children ${childMap.size}")
  }

  def archiveEdges(removalPoint:Long) = {
    println("archiving edges")
    childMap.values.foreach(child => child ! ArchiveEdges(removalPoint))
  }

  def archiveVertices(removalPoint:Long) = {
    childMap.values.foreach(child => child ! ArchiveVertices(removalPoint))
  }

  def finishedEdge(ID: Long) = {
    finishedArchiving +=1
    if(startedArchiving==finishedArchiving) {
      println("edge arciving finished, responding to parent")
      context.parent ! FinishedEdgeArchiving(finishedArchiving)
      finishedArchiving = 0
    }
  }

  def finishedVertex(ID:Int)={
    finishedArchiving +=1
    if(startedArchiving==finishedArchiving){
      println("vertex arciving finished, responding to parent")
      context.parent ! FinishedVertexArchiving(finishedArchiving)
      finishedArchiving = 0
    }
  }



}
