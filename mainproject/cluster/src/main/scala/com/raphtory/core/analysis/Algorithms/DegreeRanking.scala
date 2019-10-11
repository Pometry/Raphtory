package com.raphtory.core.analysis.Algorithms

import com.raphtory.core.analysis.API.Analyser

import scala.collection.mutable.ArrayBuffer

class DegreeRanking extends Analyser {
  override def analyse(): Any = {
    val degree =proxy.getVerticesSet().map(vert=>{
      val vertex = proxy.getVertex(vert._2)
      val outDegree = vertex.getOutgoingNeighbors.size
      val inDegree = vertex.getIngoingNeighbors.size
      (vert._1, outDegree,inDegree)
    })
    val totalV = degree.size
    val totalOut = degree.map(x=>x._2).sum
    val totalIn = degree.map(x=>x._3).sum
    val topUsers =degree.toArray.sortBy(x=> x._3).take(10)
    (totalV,totalOut,totalIn,topUsers)
  }

  override def setup(): Any = {}

  override def defineMaxSteps(): Int = 1

  override def processResults(results: ArrayBuffer[Any], oldResults: ArrayBuffer[Any], viewCompleteTime: Long): Unit = {
    println(results)
  }
}
