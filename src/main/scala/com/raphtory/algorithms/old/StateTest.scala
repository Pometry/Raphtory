package com.raphtory.algorithms.old

import scala.collection.mutable.ArrayBuffer

case class PartitionState(totalV:Int, totalDeg:Int, inDeg:Int,
                 outDeg:Int, degSq:Int,
                 vdeletionstotal:Int,vcreationstotal:Int,
                 outedgedeletionstotal:Int,outedgecreationstotal:Int,
                 inedgedeletionstotal:Int,inedgecreationstotal:Int,
                 properties:Int,propertyhistory:Int,outedgeProperties:Int,
                 outedgePropertyHistory:Int,inedgeProperties:Int,
                 inedgePropertyHistory:Int)


class StateTest(args:Array[String]) extends Analyser[PartitionState](args){

  override def analyse(): Unit = {}

  override def setup(): Unit = {}

  override def returnResults(): PartitionState = {
    val degDist = view.getVertices().map {
      vertex =>
        val inDeg = vertex.getInEdges().size
        val outDeg = vertex.getOutEdges().size
        val deg = inDeg + outDeg
        val vdeletions = vertex.history().count(f => !f.event)
        val vcreations = vertex.history().count(f =>f.event)
        val outedgedeletions =vertex.getOutEdges().map(f=>f.history().count(f => !f.event)).sum
        val outedgecreations =vertex.getOutEdges().map(f=>f.history().count(f => f.event)).sum

        val inedgedeletions =vertex.getInEdges().map(f=>f.history().count(f => !f.event)).sum
        val inedgecreations =vertex.getInEdges().map(f=>f.history().count(f => f.event)).sum

        val properties = vertex.getPropertySet().size
        val propertyhistory = vertex.getPropertySet().toArray.map(x=> vertex.getPropertyHistory(x).size).sum
        val outedgeProperties = vertex.getOutEdges().map(edge => edge.getPropertySet().size).sum
        val outedgePropertyHistory = vertex.getOutEdges().map(edge => edge.getPropertySet().toArray.map(x=> edge.getPropertyHistory(x).size).sum).sum

        val inedgeProperties = vertex.getInEdges().map(edge => edge.getPropertySet().size).sum
        val inedgePropertyHistory = vertex.getInEdges().map(edge => edge.getPropertySet().toArray.map(x=> edge.getPropertyHistory(x).size).sum).sum

        (inDeg, outDeg, deg,vdeletions,vcreations,outedgedeletions,outedgecreations,inedgedeletions,inedgecreations,properties,propertyhistory,outedgeProperties,outedgePropertyHistory,inedgeProperties,inedgePropertyHistory)
    }
    val totalV = degDist.size
    val totalDeg = degDist.map( x => x._3).sum
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

    PartitionState(totalV, totalDeg, inDeg, outDeg, degSq,
      vdeletionstotal,vcreationstotal,
      outedgedeletionstotal,outedgecreationstotal,
      inedgedeletionstotal,inedgecreationstotal,
      properties,propertyhistory,outedgeProperties,outedgePropertyHistory,inedgeProperties,inedgePropertyHistory)
  }

  override def defineMaxSteps(): Int = 1

  override def extractResults(results: List[PartitionState]): Map[String, Any] = {
    val totalVert = results.map( x => x.totalV ).sum
    val totDeg = results.map(x => x.totalDeg).sum
    val InEdge = results.map(x => x.inDeg).sum
    val OutEdge = results.map(x => x.outDeg).sum
    //val DegSq = if (totalVert > 0) endResults.map(x => x._5/totalVert.toDouble).sum else 0.0
    val vdeletionstotal = results.map( x => x.vdeletionstotal).sum
    val vcreationstotal = results.map( x => x.vcreationstotal).sum
    val outedgedeletionstotal = results.map( x => x.outedgedeletionstotal).sum
    val outedgecreationstotal = results.map( x => x.outedgecreationstotal).sum
    val inedgedeletionstotal = results.map( x => x.inedgedeletionstotal).sum
    val inedgecreationstotal = results.map( x => x.inedgecreationstotal).sum

    val properties = results.map( x => x.properties).sum
    val propertyhistory = results.map( x => x.propertyhistory).sum
    val outedgeProperties = results.map( x => x.outedgeProperties).sum
    val outedgePropertyHistory = results.map( x => x.outedgePropertyHistory).sum

    val inedgeProperties = results.map( x => x.inedgeProperties).sum
    val inedgePropertyHistory = results.map( x => x.inedgePropertyHistory).sum

    Map[String,Any]("vertices"->totalVert,"totalInEdges"->InEdge,"totalOutEdges"->OutEdge,
      "vdeletionstotal"->vdeletionstotal,"vcreationstotal"->vcreationstotal,"outedgedeletionstotal"->outedgedeletionstotal,
      "outedgecreationstotal"->outedgecreationstotal,"inedgedeletionstotal"->inedgedeletionstotal,
      "inedgecreationstotal"->inedgecreationstotal,"properties"->properties,"propertyhistory"->propertyhistory,
      "outedgeProperties"->outedgeProperties,"outedgePropertyHistory"->outedgePropertyHistory,
      "inedgeProperties"->inedgeProperties,"inedgePropertyHistory"->inedgePropertyHistory
    )
  }

}
