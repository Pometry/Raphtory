package com.raphtory.core.actors.partitionmanager.Archivist.Helpers

import akka.actor.{Actor, ActorRef, Props}
import com.raphtory.core.model.communication._
import com.raphtory.core.model.graphentities.{Edge, Vertex}
import com.raphtory.core.storage.{EntityStorage, RaphtoryDBWrite}
import monix.eval.Task

import scala.collection.parallel.mutable.ParTrieMap
class CompressionManager extends Actor{

  //timestamps to make sure all entities are compressed to exactly the same point
  var now = 0l
  val compressing    : Boolean =  System.getenv().getOrDefault("COMPRESSING", "true").trim.toBoolean
  val saving    : Boolean =  System.getenv().getOrDefault("SAVING", "true").trim.toBoolean

  var childMap = ParTrieMap[Int,ActorRef]()

  var startedCompressions = 0
  var finishedCompressions = 0
  var percentcheck = 1
  val percenting = false
  var slaveCount = 0

  override def receive:Receive = {
    case SetupSlave(children) => setup(children)
    case CompressEdges(ls) => {now = ls;compressEdges()}
    case CompressVertices(ls) => {now=ls;compressVertices()}
    case FinishedEdgeCompression(key) => finishedEdge(key)
    case FinishedVertexCompression(key) => finishedVertex(key)

  }

  //////////MAIN GUY
  def setup(children: Int) = {
    slaveCount = children
    for(i <- 0 to children){
      childMap.put(i,context.actorOf(Props[CompressionSlave],s"child_$i"))
    }
  }

  def compressEdges() = {
    println("hello")
    finishedCompressions=0
    startedCompressions=0
    var entityTotal = 0
    //val keysets = EntityStorage.edges.keySet.groupBy(key => {
    //  entityTotal +=1
    //  entityTotal % slaveCount
    //})
    //println(keysets.foreach(k=>k._2.size))
    EntityStorage.edges.keySet.foreach(key =>{
      startedCompressions +=1
      childMap.getOrElse(startedCompressions%slaveCount,null) ! CompressEdge(key,now)

    })
    println(s"starting value edges ${System.currentTimeMillis()/1000} $startedCompressions")
    percentcheck = startedCompressions/10
  }

  def compressVertices() = {
    val size = childMap.size
    finishedCompressions=0
    startedCompressions=0
    EntityStorage.vertices.keySet.foreach(key =>{
      startedCompressions +=1
      childMap.getOrElse(startedCompressions%size,null) ! CompressVertex(key,now)

    })
    println(s"starting value vertices ${System.currentTimeMillis()/1000} $startedCompressions")
    percentcheck = startedCompressions/10
  }

  def finishedEdge(key: Long) = {
    finishedCompressions +=1
    if(startedCompressions==finishedCompressions) {
      finishedCompressions=0
      startedCompressions=0
      context.parent ! FinishedEdgeCompression(finishedCompressions)

    }
  }

  def finishedVertex(key:Int)={
    finishedCompressions +=1
    if(startedCompressions==finishedCompressions) {
      finishedCompressions=0
      startedCompressions=0
      context.parent ! FinishedVertexCompression(finishedCompressions)

    }
  }

}
//if(finishedCompressions%percentcheck==0 &&startedCompressions>0&& percenting)
//    println(s"Vertex compression ${(finishedCompressions * 100) / startedCompressions;}% Complete")
//  if(finishedCompressions%percentcheck==0 &&startedCompressions>0&& percenting)
//      println(s"Edge compression ${(finishedCompressions * 100) / startedCompressions;}% Complete")