package com.raphtory.core.analysis.Algorithms

import com.raphtory.core.analysis.API.Analyser

import scala.collection.mutable.ArrayBuffer

class DegreeDistribution(args:Array[String]) extends Analyser(args){

  override def analyse(): Unit = {}

  override def setup(): Unit = {}

  override def returnResults(): Any = {
    val degDist = view.getVertices().map {
      vertex =>
        val inDeg = vertex.getIncEdges.size
        val outDeg = vertex.getOutEdges.size
        val deg = inDeg + outDeg
        (inDeg, outDeg, deg)
    }
    val totalV = degDist.size
    val totalDeg = degDist.map( x => x._3).sum
    val inDegSq = degDist.map( x => x._1 * x._1).sum
    val outDegSq = degDist.map( x => x._2 * x._2).sum
    val degSq = degDist.map( x => x._3 * x._3).sum
    (totalV, totalDeg, inDegSq, outDegSq, degSq)
  }

  override def defineMaxSteps(): Int = 1

  override def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = {
    val endResults = results.asInstanceOf[ArrayBuffer[(Int, Int, Int, Int, Int)]]
    val totalVert = endResults.map( x => x._1 ).sum
    val totDeg = endResults.map(x => x._2).sum
    val meanInDegSq = if (totalVert > 0) endResults.map(x => x._3).sum / totalVert else 0.0
    val meanOutDegSq = if (totalVert > 0) endResults.map(x => x._4).sum / totalVert else 0.0
    val meanDegSq = if (totalVert > 0) endResults.map(x => x._5).sum / totalVert else 0.0

    val text =
      s"""{"time":$timeStamp,"vertices":$totalVert,"avgSquaredDeg":$meanDegSq,"avgSquaredInDeg":$meanInDegSq,"avgSquaredOutDeg":$meanOutDegSq,"viewTime":$viewCompleteTime},"""
    println(text)
    publishData(text)
  }
}
