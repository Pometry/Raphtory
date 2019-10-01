package com.raphtory.core.components.PartitionManager.Workers

import akka.actor.{Actor, ActorRef}
import com.raphtory.core.model.communication._
import com.raphtory.core.storage.EntityStorage
import com.raphtory.core.utils.Utils

import scala.collection.parallel.mutable.ParTrieMap
class ArchivistWorker(workers:ParTrieMap[Int,ActorRef]) extends Actor{

  //timestamps to make sure all entities are compressed to exactly the same point
  val compressing    : Boolean =  Utils.compressing
  val saving    : Boolean = Utils.saving

  var startedCompressions   = 0
  var finishedCompressions  = 0
  var startedArchiving      = 0
  var finishedArchiving     = 0

  override def receive:Receive = {
    case CompressVertices(compressTime,workerID) => {compressVertices(compressTime,workerID)}
    case ArchiveVertices(compressTime,archiveTime,workerID) => {archiveVertices(compressTime,archiveTime,workerID)}
    case FinishedVertexCompression(key) => finishedVertexCompression(key)
    case FinishedVertexArchiving(key) => finishedVertexArchiving(key)
  }

  def compressVertices(compressTime:Long,workerID:Int) = {
     val worker = workers(workerID)
      EntityStorage.vertices(workerID) foreach( pair => {
        worker ! CompressVertex(pair._1,compressTime)
        startedCompressions+=1
      })
   }

  def archiveVertices(compressTime:Long,archiveTime:Long,workerID:Int) = {
      val worker = workers(workerID)
      EntityStorage.vertices(workerID) foreach( key => {
        if(compressing) worker ! ArchiveVertex(key._1,compressTime,archiveTime)
        else worker ! ArchiveOnlyVertex(key._1,archiveTime)
        startedArchiving+=1
      })
  }

  def finishedVertexCompression(key:Int)={
    finishedCompressions+=1
    if(startedCompressions==finishedCompressions) {
      context.parent ! FinishedVertexCompression(startedCompressions)
      startedCompressions = 0
      finishedCompressions = 0
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