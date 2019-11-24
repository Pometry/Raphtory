package com.raphtory.core.analysis.Algorithms

import com.raphtory.core.analysis.API.Analyser

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.immutable
import scala.util.Random

class BinaryDefusion extends Analyser{
  val infectedNode = 6585
  override def setup(): Unit = {
    proxy.getVerticesSet().foreach(v => {
      if(v._1 == infectedNode) {
        val vertex = proxy.getVertex(v._2)
        val toSend = vertex.getOrSetCompValue("infected", proxy.superStep()).asInstanceOf[Int]
        vertex.getOutgoingNeighbors.foreach(neighbour => {
          if(Random.nextBoolean())
            vertex.messageNeighbour(neighbour._1,toSend)
        })
      }
    })
  }

  override def analyse(): Unit = {
    proxy.getVerticesWithMessages().foreach(vert=>{
      val vertex = proxy.getVertex(vert._2)
      vertex.clearQueue
      if(vertex.containsCompValue("infected")){
        vertex.voteToHalt() //already infected
      }
      else{
        val toSend = vertex.getOrSetCompValue("infected", proxy.superStep()).asInstanceOf[Int]
        vertex.getOutgoingNeighbors.foreach(neighbour => {
          if(Random.nextBoolean())
            vertex.messageNeighbour(neighbour._1,toSend)
        })
      }
    })
  }

  override def returnResults(): Any = {
    proxy.getVerticesSet().map(vert=>{
      val vertex = proxy.getVertex(vert._2)
      if(vertex.containsCompValue("infected"))
        (vert._1,vertex.getCompValue("infected").asInstanceOf[Int])
      else
        (-1,-1)

    }).filter(f=> f._2 >=0)
  }

  override def defineMaxSteps(): Int = 100

  override def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = ???

  override def processViewResults(results: ArrayBuffer[Any], timestamp: Long, viewCompleteTime: Long): Unit = {
    val endResults = results.asInstanceOf[ArrayBuffer[immutable.ParHashMap[Long, Int]]].flatten
    println(endResults)
    println(endResults.size)
  }
}
