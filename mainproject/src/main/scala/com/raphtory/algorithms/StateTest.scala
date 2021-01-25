package com.raphtory.algorithms

import com.raphtory.api.Analyser

import scala.collection.mutable.ArrayBuffer

class StateTest(args:Array[String]) extends Analyser(args){

  override def analyse(): Unit = {}

  override def setup(): Unit = {}

  override def returnResults(): Any = {
    val degDist = view.getVertices().map {
      vertex =>
        val inDeg = vertex.getIncEdges.size
        val outDeg = vertex.getOutEdges.size
        val deg = inDeg + outDeg
        val vdeletions = vertex.getHistory().count(f =>f._2 ==false)
        val vcreations = vertex.getHistory().count(f =>f._2 ==true)
        val outedgedeletions =vertex.getOutEdges.map(f=>f.getHistory().count(f =>f._2 ==false)).sum
        val outedgecreations =vertex.getOutEdges.map(f=>f.getHistory().count(f =>f._2 ==true)).sum
        (inDeg, outDeg, deg,vdeletions,vcreations,outedgedeletions,outedgecreations)
    }
    val totalV = degDist.size
    val totalDeg = degDist.map( x => x._3).sum
    val maxDeg = if (degDist.size > 0) degDist.map(x => x._3).max else 0
    val inDegSq = degDist.map( x => x._1 * x._1).sum
    val outDegSq = degDist.map( x => x._2 * x._2).sum
    val degSq = degDist.map( x => x._3 * x._3).sum

    val vdeletionstotal = degDist.map( x => x._4).sum
    val vcreationstotal = degDist.map( x => x._5).sum
    val outedgedeletionstotal = degDist.map( x => x._6).sum
    val outedgecreationstotal = degDist.map( x => x._7).sum

    (totalV, totalDeg, inDegSq, outDegSq, degSq, maxDeg,vdeletionstotal,vcreationstotal,outedgedeletionstotal,outedgecreationstotal)
  }

  override def defineMaxSteps(): Int = 1

  override def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = {
    val endResults = results.asInstanceOf[ArrayBuffer[(Int, Int, Int, Int, Int, Int,Int,Int,Int,Int)]]
    val totalVert = endResults.map( x => x._1 ).sum
    val totDeg = endResults.map(x => x._2).sum
    val maxDeg = endResults.map(x => x._6).max
    val meanInDegSq = if (totalVert > 0) endResults.map(x => x._3/totalVert.toDouble).sum else 0.0
    val meanOutDegSq = if (totalVert > 0) endResults.map(x => x._4/totalVert.toDouble).sum else 0.0
    val meanDegSq = if (totalVert > 0) endResults.map(x => x._5/totalVert.toDouble).sum else 0.0
    val vdeletionstotal = endResults.map( x => x._7).sum
    val vcreationstotal = endResults.map( x => x._8).sum
    val outedgedeletionstotal = endResults.map( x => x._9).sum
    val outedgecreationstotal = endResults.map( x => x._10).sum

    val text =
      s"""{"time":$timeStamp,"vertices":$totalVert, "maxDeg":$maxDeg,"avgSquaredDeg":$meanDegSq,"avgSquaredInDeg":$meanInDegSq,"avgSquaredOutDeg":$meanOutDegSq,"vdeletionstotal":$vdeletionstotal,"vcreationstotal":$vcreationstotal,"outedgedeletionstotal":$outedgedeletionstotal,"outedgecreationstotal":$outedgecreationstotal}"""
    publishData(text)
  }

  override def processWindowResults(results: ArrayBuffer[Any], timestamp: Long, windowSize: Long,
                                    viewCompleteTime: Long ):
  Unit = {
    var output_folder = System.getenv().getOrDefault("OUTPUT_FOLDER", "/app").trim
    var output_file = output_folder + "/" + System.getenv().getOrDefault("OUTPUT_FILE","DegreeDistribution.json").trim
    val endResults = results.asInstanceOf[ArrayBuffer[(Int, Int, Int, Int, Int, Int,Int, Int, Int, Int)]]
    val totalVert = endResults.map( x => x._1 ).sum
    val totDeg = endResults.map(x => x._2).sum
    val maxDeg = endResults.map(x => x._6).max
    val meanInDegSq = if (totalVert > 0) endResults.map(x => x._3/totalVert.toDouble).sum else 0.0
    val meanOutDegSq = if (totalVert > 0) endResults.map(x => x._4/totalVert.toDouble).sum else 0.0
    val meanDegSq = if (totalVert > 0) endResults.map(x => x._5/totalVert.toDouble).sum else 0.0
    val vdeletionstotal = endResults.map( x => x._7).sum
    val vcreationstotal = endResults.map( x => x._8).sum
    val outedgedeletionstotal = endResults.map( x => x._9).sum
    val outedgecreationstotal = endResults.map( x => x._10).sum

    val text =
      s"""{"time":$timestamp,"windowsize":$windowSize,"vertices":$totalVert,"maxDeg":$maxDeg,"avgSquaredDeg":$meanDegSq,"avgSquaredInDeg":$meanInDegSq,"avgSquaredOutDeg":$meanOutDegSq,"vdeletionstotal":$vdeletionstotal,"vcreationstotal":$vcreationstotal,"outedgedeletionstotal":$outedgedeletionstotal,"outedgecreationstotal":$outedgecreationstotal}"""
    println(text)
    publishData(text)
  }
}
