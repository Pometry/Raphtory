package com.raphtory.algorithms

import com.raphtory.core.analysis.api.Analyser

import scala.collection.mutable.ArrayBuffer

class DegreeDistribution(args:Array[String]) extends Analyser[Any](args){

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
    val maxDeg = if (degDist.size > 0) degDist.map(x => x._3).max else 0
    val inDegSq = degDist.map( x => x._1 * x._1).sum
    val outDegSq = degDist.map( x => x._2 * x._2).sum
    val degSq = degDist.map( x => x._3 * x._3).sum
    (totalV, totalDeg, inDegSq, outDegSq, degSq, maxDeg)
  }

  override def defineMaxSteps(): Int = 1

  override def extractResults(results: Array[Any]): Any = {
    val endResults = results.asInstanceOf[ArrayBuffer[(Int, Int, Int, Int, Int, Int)]]
    val totalVert = endResults.map( x => x._1 ).sum
    val totDeg = endResults.map(x => x._2).sum
    val maxDeg = endResults.map(x => x._6).max
    val meanInDegSq = if (totalVert > 0) endResults.map(x => x._3/totalVert.toDouble).sum else 0.0
    val meanOutDegSq = if (totalVert > 0) endResults.map(x => x._4/totalVert.toDouble).sum else 0.0
    val meanDegSq = if (totalVert > 0) endResults.map(x => x._5/totalVert.toDouble).sum else 0.0

    val text =
      s"""{"vertices":$totalVert, "maxDeg":$maxDeg,"avgSquaredDeg":$meanDegSq,"avgSquaredInDeg":$meanInDegSq,"avgSquaredOutDeg":$meanOutDegSq}"""
    println(text)
    publishData(text)
  }

}
