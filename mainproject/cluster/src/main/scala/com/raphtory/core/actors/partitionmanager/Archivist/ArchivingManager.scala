package com.raphtory.core.actors.partitionmanager.Archivist

import akka.actor.{Actor, ActorRef}
import com.raphtory.core.model.communication._

import scala.collection.parallel.mutable.ParTrieMap

class ArchivingManager(Workers:ParTrieMap[Int,ActorRef]) extends Actor{
  var startedArchiving = 10
  var finishedArchiving = 0

  override def receive:Receive = {
    case ArchiveEdges(ls) => {archiveEdges(ls)}
    case ArchiveVertices(ls) => {archiveVertices(ls)}
    case FinishedEdgeArchiving(key) => finishedEdge(key)
    case FinishedVertexArchiving(key) => finishedVertex(key)
  }



  def archiveEdges(removalPoint:Long) = {
    println("archiving edges")
    Workers.values.foreach(child => child ! ArchiveEdges(removalPoint))
  }

  def archiveVertices(removalPoint:Long) = {
    Workers.values.foreach(child => child ! ArchiveVertices(removalPoint))
  }

  def finishedEdge(ID: Long) = {
    finishedArchiving +=1
    if(startedArchiving==finishedArchiving) {
      println("edge archiving finished, responding to parent")
      context.parent ! FinishedEdgeArchiving(finishedArchiving)
      finishedArchiving = 0
    }
  }

  def finishedVertex(ID:Int)={
    finishedArchiving +=1
    if(startedArchiving==finishedArchiving){
      println("vertex archiving finished, responding to parent")
      context.parent ! FinishedVertexArchiving(finishedArchiving)
      finishedArchiving = 0
    }
  }



}
