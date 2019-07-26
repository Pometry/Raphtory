package com.raphtory.core.analysis

import akka.actor.ActorContext
import com.raphtory.core.model.graphentities.Vertex
import com.raphtory.core.storage.{EntityStorage, RaphtoryDBRead}
import monix.execution.atomic.AtomicInt

import scala.collection.parallel
import scala.collection.parallel.ParSet
import scala.collection.parallel.mutable.ParTrieMap

class GraphRepoProxy(jobID:String,superstep:Int) {
  private var messages = AtomicInt(0)
  private var voteCount = 0
  def job() = jobID
  def getVerticesSet()(implicit workerID:WorkerID): Array[Int] = {
    EntityStorage.vertexKeys(workerID.ID).toArray
  }

  def recordMessage() = messages.increment()
  def getMessages() = messages.get
  def clearMessages() = messages.set(0)


  def getVertex(id : Long)(implicit context : ActorContext, managerCount : ManagerCount) : VertexVisitor = new VertexVisitor(EntityStorage.vertices(id.toInt),jobID,superstep,this)

  def getTotalVerticesSet()  = {
    EntityStorage.vertices.keySet
  }

  def latestTime:Long = EntityStorage.newestTime

  def vertexVoted() = voteCount +=1

  def checkVotes(workerID: Int):Boolean = {
    //println(s"$workerID ${EntityStorage.vertexKeys(workerID).size} $voteCount")
    EntityStorage.vertexKeys(workerID).size == voteCount
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
