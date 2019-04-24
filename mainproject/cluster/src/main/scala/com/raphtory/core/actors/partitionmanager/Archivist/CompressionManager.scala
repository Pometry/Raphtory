package com.raphtory.core.actors.partitionmanager.Archivist

import akka.actor.{Actor, ActorRef}
import com.raphtory.core.model.communication._

import scala.collection.parallel.mutable.ParTrieMap
class CompressionManager(workers:ParTrieMap[Int,ActorRef]) extends Actor{

  //timestamps to make sure all entities are compressed to exactly the same point
  var now = 0l
  val compressing    : Boolean =  System.getenv().getOrDefault("COMPRESSING", "true").trim.toBoolean
  val saving    : Boolean =  System.getenv().getOrDefault("SAVING", "true").trim.toBoolean

  var startedCompressions = 0
  var finishedCompressions = 0

  override def receive:Receive = {
    case CompressEdges(ls) => {now = ls;compressEdges()}
    case CompressVertices(ls) => {now=ls;compressVertices()}
    case FinishedEdgeCompression(key) => finishedEdge(key)
    case FinishedVertexCompression(key) => finishedVertex(key)

  }

  def compressEdges() = {
    startedCompressions = workers.size
    workers.values.foreach(child => child ! CompressEdges(now))
  }

  def compressVertices() = {
    startedCompressions = workers.size
    workers.values.foreach(child => child ! CompressVertices(now))
  }

  def finishedEdge(key: Long) = {
    finishedCompressions +=1
    if(startedCompressions==finishedCompressions) {
      finishedCompressions=0
      context.parent ! FinishedEdgeCompression(finishedCompressions)

    }
  }

  def finishedVertex(key:Int)={
    finishedCompressions +=1
    if(startedCompressions==finishedCompressions) {
      finishedCompressions=0
      context.parent ! FinishedVertexCompression(finishedCompressions)

    }
  }
}
//  var percentcheck = 1
//val percenting = false
//if(finishedCompressions%percentcheck==0 &&startedCompressions>0&& percenting)
//    println(s"Vertex compression ${(finishedCompressions * 100) / startedCompressions;}% Complete")
//  if(finishedCompressions%percentcheck==0 &&startedCompressions>0&& percenting)
//      println(s"Edge compression ${(finishedCompressions * 100) / startedCompressions;}% Complete")