package com.raphtory.core.components.PartitionManager.Workers

import akka.actor.{Actor, ActorRef}
import com.raphtory.core.model.communication._
import com.raphtory.core.storage.EntityStorage

import scala.collection.parallel.mutable.ParTrieMap
class ArchivistWorker(workers:ParTrieMap[Int,ActorRef]) extends Actor{

  //timestamps to make sure all entities are compressed to exactly the same point

  val compressing    : Boolean =  System.getenv().getOrDefault("COMPRESSING", "true").trim.toBoolean
  val saving    : Boolean =  System.getenv().getOrDefault("SAVING", "false").trim.toBoolean

  var startedCompressions   = 0
  var finishedCompressions  = 0
  var startedArchiving      = 0
  var finishedArchiving     = 0

  override def receive:Receive = {
    case CompressEdges(compressTime) => {compressEdges(compressTime)}
    case CompressVertices(compressTime) => {compressVertices(compressTime)}
    case FinishedEdgeCompression(key) => finishedEdgeCompression(key)
    case FinishedVertexCompression(key) => finishedVertexCompression(key)

    case ArchiveEdges(compressTime,archiveTime) => {archiveEdges(compressTime,archiveTime)}
    case ArchiveVertices(compressTime,archiveTime) => {archiveVertices(compressTime,archiveTime)}
    case FinishedEdgeArchiving(key) => finishedEdgeArchiving(key)
    case FinishedVertexArchiving(key) => finishedVertexArchiving(key)

  }

  def compressEdges(compressTime:Long) = {
    for (workerID <- 0 until 10) {
      val worker = workers(workerID)
      EntityStorage.edgeKeys(workerID) foreach(key => {
        worker ! CompressEdge(key,compressTime)
        startedCompressions+=1
      })
    }
  }

  def compressVertices(compressTime:Long) = {
    for (workerID <- 0 until 10){
     val worker = workers(workerID)
      EntityStorage.vertexKeys(workerID) foreach( key => {
        worker ! CompressVertex(key,compressTime)
        startedCompressions+=1
      })
    }
  }

  def archiveEdges(compressTime:Long,archiveTime:Long) = {
    for (workerID <- 0 until 10){
      val worker = workers(workerID)
      EntityStorage.edgeKeys(workerID) foreach( key => {
        if(compressing)
          worker ! ArchiveEdge(key,compressTime,archiveTime)
        else
          worker ! ArchiveOnlyEdge(key,archiveTime)

        startedArchiving+=1
      })
    }
  }

  def archiveVertices(compressTime:Long,archiveTime:Long) = {
    for (workerID <- 0 until 10) {
      val worker = workers(workerID)
      EntityStorage.vertexKeys(workerID) foreach( key => {
        if(compressing)
          worker ! ArchiveVertex(key,compressTime,archiveTime)
        else
          worker ! ArchiveOnlyVertex(key,archiveTime)
        startedArchiving+=1
      })
    }
  }

  def finishedEdgeCompression(key: Long) = {
    finishedCompressions+=1
    if(startedCompressions==finishedCompressions) {
      context.parent ! FinishedEdgeCompression(startedCompressions)
      startedCompressions = 0
      finishedCompressions = 0
    }
  }

  def finishedVertexCompression(key:Int)={
    finishedCompressions+=1
    if(startedCompressions==finishedCompressions) {
      context.parent ! FinishedVertexCompression(startedCompressions)
      startedCompressions = 0
      finishedCompressions = 0
    }
  }

  def finishedEdgeArchiving(ID: Long) = {
    finishedArchiving +=1
    if(startedArchiving==finishedArchiving) {
      context.parent ! FinishedEdgeArchiving(startedArchiving)
      startedArchiving= 0
      finishedArchiving = 0
    }
  }

  def finishedVertexArchiving(ID:Int)={
    finishedArchiving+=1
    if(startedArchiving==finishedArchiving){
      context.parent ! FinishedVertexArchiving(startedArchiving)
      startedArchiving = 0
      finishedArchiving = 0
    }
  }
}
//  var percentcheck = 1
//val percenting = false
//if(finishedCompressions%percentcheck==0 &&startedCompressions>0&& percenting)
//    println(s"Vertex compression ${(finishedCompressions * 100) / startedCompressions;}% Complete")
//  if(finishedCompressions%percentcheck==0 &&startedCompressions>0&& percenting)
//      println(s"Edge compression ${(finishedCompressions * 100) / startedCompressions;}% Complete")