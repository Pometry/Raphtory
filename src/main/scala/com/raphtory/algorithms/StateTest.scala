package com.raphtory.algorithms

import com.raphtory.core.analysis.api.Analyser

import scala.collection.mutable.ArrayBuffer

class StateTest(args:Array[String]) extends Analyser[Any](args){

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

        val inedgedeletions =vertex.getIncEdges.map(f=>f.getHistory().count(f =>f._2 ==false)).sum
        val inedgecreations =vertex.getIncEdges.map(f=>f.getHistory().count(f =>f._2 ==true)).sum

        val properties = vertex.getPropertySet().size
        val propertyhistory = vertex.getPropertySet().keys.toArray.map(x=> vertex.getPropertyHistory(x).size).sum
        val outedgeProperties = vertex.getOutEdges.map(edge => edge.getPropertySet().size).sum
        val outedgePropertyHistory = vertex.getOutEdges.map(edge => edge.getPropertySet().keys.toArray.map(x=> edge.getPropertyHistory(x).size).sum).sum

        val inedgeProperties = vertex.getIncEdges.map(edge => edge.getPropertySet().size).sum
        val inedgePropertyHistory = vertex.getIncEdges.map(edge => edge.getPropertySet().keys.toArray.map(x=> edge.getPropertyHistory(x).size).sum).sum

        (inDeg, outDeg, deg,vdeletions,vcreations,outedgedeletions,outedgecreations,inedgedeletions,inedgecreations,properties,propertyhistory,outedgeProperties,outedgePropertyHistory,inedgeProperties,inedgePropertyHistory)
    }
    val totalV = degDist.size
    val totalDeg = degDist.map( x => x._3).sum
    val maxDeg = if (degDist.size > 0) degDist.map(x => x._3).max else 0
    val inDeg = degDist.map( x => x._1).sum
    val outDeg = degDist.map( x => x._2).sum
    val degSq = degDist.map( x => x._3).sum

    val vdeletionstotal = degDist.map( x => x._4).sum
    val vcreationstotal = degDist.map( x => x._5).sum
    val outedgedeletionstotal = degDist.map( x => x._6).sum
    val outedgecreationstotal = degDist.map( x => x._7).sum

    val inedgedeletionstotal = degDist.map( x => x._8).sum
    val inedgecreationstotal = degDist.map( x => x._9).sum

    val properties = degDist.map( x => x._10).sum
    val propertyhistory = degDist.map( x => x._11).sum
    val outedgeProperties = degDist.map( x => x._12).sum
    val outedgePropertyHistory = degDist.map( x => x._13).sum

    val inedgeProperties = degDist.map( x => x._14).sum
    val inedgePropertyHistory = degDist.map( x => x._15).sum

    (totalV, totalDeg, inDeg, outDeg, degSq, maxDeg,
      vdeletionstotal,vcreationstotal,
      outedgedeletionstotal,outedgecreationstotal,
      inedgedeletionstotal,inedgecreationstotal,
      properties,propertyhistory,outedgeProperties,outedgePropertyHistory,inedgeProperties,inedgePropertyHistory)
  }

  override def defineMaxSteps(): Int = 1

  override def extractResults(results: Array[Any]): Any = {
    val endResults = results.asInstanceOf[ArrayBuffer[(Int, Int, Int, Int, Int, Int,Int,Int,Int,Int, Int, Int, Int, Int,Int,Int,Int,Int)]]
    val totalVert = endResults.map( x => x._1 ).sum
    val totDeg = endResults.map(x => x._2).sum
    val maxDeg = endResults.map(x => x._6).max
    val InEdge = endResults.map(x => x._3).sum
    val OutEdge = endResults.map(x => x._4).sum
    //val DegSq = if (totalVert > 0) endResults.map(x => x._5/totalVert.toDouble).sum else 0.0
    val vdeletionstotal = endResults.map( x => x._7).sum
    val vcreationstotal = endResults.map( x => x._8).sum
    val outedgedeletionstotal = endResults.map( x => x._9).sum
    val outedgecreationstotal = endResults.map( x => x._10).sum
    val inedgedeletionstotal = endResults.map( x => x._11).sum
    val inedgecreationstotal = endResults.map( x => x._12).sum

    val properties = endResults.map( x => x._13).sum
    val propertyhistory = endResults.map( x => x._14).sum
    val outedgeProperties = endResults.map( x => x._15).sum
    val outedgePropertyHistory = endResults.map( x => x._16).sum

    val inedgeProperties = endResults.map( x => x._17).sum
    val inedgePropertyHistory = endResults.map( x => x._18).sum

    val text =
      s"""{"vertices":$totalVert, "maxDeg":$maxDeg,
         |"totalInEdges":$InEdge,"totalOutEdges":$OutEdge,"vdeletionstotal":$vdeletionstotal,
         |"vcreationstotal":$vcreationstotal,"outedgedeletionstotal":$outedgedeletionstotal,"outedgecreationstotal":$outedgecreationstotal,
         |"inedgedeletionstotal":$inedgedeletionstotal,"inedgecreationstotal":$inedgecreationstotal,"properties":$properties,
         |"propertyhistory":$propertyhistory,"outedgeProperties":$outedgeProperties,"outedgePropertyHistory":$outedgePropertyHistory,
         |"inedgeProperties":$inedgeProperties,"inedgePropertyHistory":$inedgePropertyHistory}""".stripMargin
    publishData(text)
  }

}
