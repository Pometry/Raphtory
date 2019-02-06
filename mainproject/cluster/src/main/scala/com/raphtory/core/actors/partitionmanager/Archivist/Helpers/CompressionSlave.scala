package com.raphtory.core.actors.partitionmanager.Archivist.Helpers

import akka.actor.{Actor, ActorRef, Props}
import com.raphtory.core.model.communication._
import com.raphtory.core.model.graphentities.{Edge, Vertex}
import com.raphtory.core.storage.{EntityStorage, RaphtoryDBWrite}
import monix.eval.Task

import scala.collection.parallel.mutable.ParTrieMap
import scala.util.{Failure, Success}
class CompressionSlave extends Actor{

  //timestamps to make sure all entities are compressed to exactly the same point
  var now = 0l
  var compressionPercent = 90f
  val compressing    : Boolean =  System.getenv().getOrDefault("COMPRESSING", "true").trim.toBoolean
  val saving    : Boolean =  System.getenv().getOrDefault("SAVING", "true").trim.toBoolean

  var childMap = ParTrieMap[Int,ActorRef]()

  var startedCompressions = 0
  var finishedCompressions = 0
  var lastMapsize = 0

  override def receive:Receive = {
    case SetupSlave(children) => setup(children)
    case CompressEdges(ls) => {now = ls;compressEdges()}
    case CompressEdge(key,time) => compressEdge(key,time)
    case FinishedEdgeCompression(key) => finishedEdge(key)
    case CompressVertices(ls) => {now=ls;compressVertices()}
    case CompressVertex(key,time) => compressVertex(key,time)
    case FinishedVertexCompression(key) => finishedVertex(key)
  }

  //////////MAIN GUY
  def setup(children: Int) = {
    for(i <- 0 to children){
      childMap.put(i,context.actorOf(Props[CompressionSlave],s"child_$i"))
    }
  }

  def compressEdges() = {
    lastMapsize = EntityStorage.edges.size
    println(lastMapsize)
    val size = childMap.size
    EntityStorage.edges.keySet.foreach(key =>{
      startedCompressions +=1
      childMap.getOrElse(startedCompressions%size,null) ! compressEdge(key,now)

    })
  }

  def compressVertices() = {
    lastMapsize = EntityStorage.vertices.size
    val size = childMap.size
    EntityStorage.vertices.keySet.foreach(key =>{
      startedCompressions +=1
      childMap.getOrElse(startedCompressions%size,null) ! compressVertex(key,now)

    })
    println(s"starting value $startedCompressions")
  }

  def finishedEdge(key: Long) = {
    finishedCompressions +=1
    if(startedCompressions==finishedCompressions && finishedCompressions >= lastMapsize) {
      context.parent ! FinishedEdgeCompression(finishedCompressions)
      finishedCompressions=0
      startedCompressions=0
    }
  }

  def finishedVertex(key:Int)={
    finishedCompressions +=1
    if(startedCompressions==finishedCompressions && finishedCompressions >= lastMapsize) {
      context.parent ! FinishedVertexCompression(finishedCompressions)
      finishedCompressions=0
      startedCompressions=0
    }
  }

  //////////END MAIN GUY


  //SLAVE SLAVE

  def compressEdge(key:Long,now:Long) = {
      EntityStorage.edges.synchronized {
        EntityStorage.edges.get(key) match {
          case Some(edge) => saveEdge(edge, now)
          case None => //do nothing
        }
      }
    context.parent ! FinishedEdgeCompression(key)
  }

  def compressVertex(key:Int,now:Long) = {
    EntityStorage.vertices.synchronized {
      EntityStorage.vertices.get(key) match {
        case Some(vertex) => saveVertex(vertex, now)
        case None => //do nothing
      }
    }
    context.parent ! FinishedVertexCompression(key)
  }



  def saveEdge(edge:Edge,cutOff:Long) ={
    val history = edge.compressAndReturnOldHistory(cutOff)
    if(saving) {
      if(history.size > 0) {
        RaphtoryDBWrite.edgeHistory.save(edge.getSrcId, edge.getDstId, history)
      }
      edge.properties.foreach(property => {
        val propHistory = property._2.compressAndReturnOldHistory(cutOff)
        if(propHistory.size > 0) {
          RaphtoryDBWrite.edgePropertyHistory.save(edge.getSrcId, edge.getDstId, property._1, propHistory)
        }
      })
    }
  }

  def saveVertex(vertex:Vertex,cutOff:Long) = {
    val history = vertex.compressAndReturnOldHistory(cutOff)
    if(saving) { //if we are saving data to cassandra
      if (history.size > 0) {
        RaphtoryDBWrite.vertexHistory.save(vertex.getId, history)
      }
      vertex.properties.foreach(prop => {
        val propHistory = prop._2.compressAndReturnOldHistory(cutOff)
        if (propHistory.size > 0) {
          RaphtoryDBWrite.vertexPropertyHistory.save(vertex.getId, prop._1, propHistory)
        }
      })
    }
  }









}
