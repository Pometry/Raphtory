package com.raphtory.core.analysis.API.GraphRepositoryProxies

import java.util.concurrent.ConcurrentHashMap

import akka.actor.ActorContext
import com.raphtory.core.analysis.API.VertexVisitor
import com.raphtory.core.analysis.API.{ManagerCount, WorkerID}
import com.raphtory.core.model.communication.VertexMessage
import com.raphtory.core.model.graphentities.Vertex
import com.raphtory.core.storage.EntityStorage
import com.raphtory.core.utils.Utils
import monix.execution.atomic.AtomicInt

import scala.collection.parallel.mutable.ParTrieMap
import scala.collection.parallel.{ParIterable, ParSet, mutable}

class LiveProxy(jobID:String, superstep:Int, timestamp:Long, windowsize:Long,workerID: Int,storage:EntityStorage,managerCount: ManagerCount) {
  private var messages = AtomicInt(0)
  //val messageQueues:ParTrieMap[String,ConcurrentHashMap.KeySetView[(Long,Long,Any),java.lang.Boolean]] = ParTrieMap[String,ConcurrentHashMap.KeySetView[(Long,Long,Any),java.lang.Boolean]]()
  //Utils.getAllReaders(managerCount.count).foreach(reader => messageQueues put (reader,java.util.concurrent.ConcurrentHashMap.newKeySet[(Long,Long,Any)]()))

  protected var voteCount = AtomicInt(0)
  def job() = jobID

  def getVerticesSet(): ParTrieMap[Long,Vertex] = storage.vertices

  def getVerticesWithMessages(): ParTrieMap[Long,Vertex] = storage.vertices.filter{case (id:Long,vertex:Vertex) => vertex.multiQueue.getMessageQueue(job(),superstep).nonEmpty}

  def recordMessage(sourceID:Long,vertexID:Long,data:Any) = {
    //messageQueues(Utils.getReader(vertexID, managerCount.count)) add ((sourceID,vertexID,data))
    messages.increment()
  }

  def getMessages() = messages.get

  def getVertex(v : Vertex)(implicit context : ActorContext, managerCount : ManagerCount) : VertexVisitor = new VertexVisitor(v,jobID,superstep,this,timestamp,windowsize)

  def getTotalVerticesSet() = {
    storage.vertices.keySet
  }

  def latestTime:Long = storage.newestTime

  def vertexVoted() = voteCount.increment()

  def checkVotes(workerID: Int):Boolean = {
    //
   storage.vertices.size == voteCount.get
  }

  //HERE BE DRAGONS please ignore
  //  def something= {
  //    val compress = EntityStorage.lastCompressedAt
  //    println(s"compression time = $compress")
  //    val verticesInMem = EntityStorage.createSnapshot(compress)
  //    val loadedVertices = ParTrieMap[Int, Vertex]()
  //    var startCount = 0
  //    val finishCount = AtomicInt(0)
  //    var retryQueue:parallel.mutable.ParHashSet[Long] = parallel.mutable.ParHashSet.empty
  //    for (id <- GraphRepoProxy.getVerticesSet()){
  //      startCount +=1
  //      RaphtoryDBRead.retrieveVertex(id.toLong,compress,loadedVertices,finishCount,retryQueue)
  //      if((startCount % 1000) == 0)
  //        while(startCount> finishCount.get){
  //          Thread.sleep(1) //Throttle requests to cassandra
  //        }
  //    }
  //    println("queue size"+retryQueue.size)
  //    if(!retryQueue.isEmpty){
  //      rerun(finishCount,retryQueue,compress,loadedVertices)
  //    }
  //
  //
  //    val loadedVertices2 = ParTrieMap[Int, Vertex]()
  //    startCount = 0
  //    finishCount.set(0)
  //    var retryQueue2:parallel.mutable.ParHashSet[Long] = parallel.mutable.ParHashSet.empty
  //    for (id <- GraphRepoProxy.getVerticesSet()){
  //      startCount +=1
  //      RaphtoryDBRead.retrieveVertex(id.toLong,compress,loadedVertices2,finishCount,retryQueue2)
  //      if((startCount % 1000) == 0)
  //        while(startCount> finishCount.get){
  //          Thread.sleep(1) //Throttle requests to cassandra
  //        }
  //    }
  //    println("queue size"+retryQueue2.size)
  //    if(!retryQueue2.isEmpty){
  //      rerun(finishCount,retryQueue2,compress,loadedVertices2)
  //    }
  //
  //
  //    //    while(startCount> finishCount.get){
  ////      Thread.sleep(100)
  ////      println(System.currentTimeMillis())
  ////      println(retryQueue.size)
  ////
  ////    }
  //
  //
  //    println(verticesInMem.size)
  //    println(loadedVertices.size)
  //    println(loadedVertices2.size)
  //    for((k,v) <- verticesInMem){
  //      loadedVertices.get(k) match {
  //        case Some(e) =>
  //        case None => RaphtoryDBRead.retrieveVertex(v.vertexId,compress,loadedVertices,finishCount,parallel.mutable.ParHashSet.empty)
  //      }
  //    }
  //    Thread.sleep(10000)
  //    println(loadedVertices.size)
  //    println(verticesInMem.equals(loadedVertices))
  //  }
  //def iterativeApply(f : Connector => Unit)

  //  def rerun(finishCount:AtomicInt,retryQueue:parallel.mutable.ParHashSet[Long],compress:Long,loadedVertices:ParTrieMap[Int, Vertex]): Unit ={
  //    var startCount = 0
  //    finishCount.set(0)
  //    println(s"map size ${loadedVertices.size}")
  //    val retryQueueNew:parallel.mutable.ParHashSet[Long] = parallel.mutable.ParHashSet.empty
  //    retryQueue.foreach(id => {
  //      startCount +=1
  //      RaphtoryDBRead.retrieveVertex(id,compress,loadedVertices,finishCount,retryQueueNew)
  //      if((startCount % 1000) == 0)
  //        println("queue size"+retryQueue.size)
  //      while(startCount> finishCount.get){
  //       // println(s"map size ${loadedVertices.size}")
  //        Thread.sleep(1) //Throttle requests to cassandra
  //      }
  //    })
  //
  //    println("queue size"+retryQueueNew.size)
  //    println(s"map size ${loadedVertices.size}")
  //    if(!retryQueueNew.isEmpty){
  //      rerun(finishCount,retryQueueNew,compress,loadedVertices)
  //    }
  //  }


  //  def addEdge(id : Long) = {
  //    edgesSet += id
  //  }



  //  def getEdgesSet() : ParSet[Long] = {
  //    edgesSet
  //  }



  //  def getEdge(id : Long) : Edge = {
  //
  //    return null
  //  }
}