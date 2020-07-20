package com.raphtory.core.analysis.Algorithms

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.utils.Utils

import scala.collection.mutable.ArrayBuffer

class FlowGraph(args:Array[String]) extends Analyser(args){

  override def setup(): Unit = {}

  override def analyse(): Unit = {}

  override def returnResults(): Any = {
    val locV = view.getVerticesSet().filter{ vertex => vertex.Type() == "Location" }
    if(locV.size > 1){
      var flow = List[(Long, Long, Int)]()
      //TODO Revisit with Imane
      locV.foreach { vertU =>
//        val neighU = vertU.getIncEdges.keySet
//        val excU = locV - vertU.ID()
//        excU.foreach { v =>
//          val neighV = view.getVertex(v._2).getIncEdges.keySet
//          val com = (neighU & neighV).size
//          flow = (u._2.getId, v._2.getId, com) :: flow
//        }
//      }
      flow
    }
    }
  }

  override def defineMaxSteps(): Int = 100

  override def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = {
      val endResults  = results.filter(_ != (())).asInstanceOf[ArrayBuffer[List[(Long, Long, Int)]]].filter(!_.isEmpty).flatten
      val output_file = System.getenv().getOrDefault("PROJECT_OUTPUT", "/home/tsunade/qmul/results/flow-output.csv").trim
      val startTime   = System.currentTimeMillis()
    try {
      val totalV = endResults.map(x=>x._1).toSet.size
      val totalE = endResults.map(x => x._1).size
      val maxFlow = if (endResults.map(_._3).nonEmpty) endResults.map(_._3).max else 0
      val busyEdgeArray = endResults.sortBy(_._3)(Ordering[Int].reverse).map(x=>(x._1,x._2)).take(5)
      val text =
        s"""{"time":$timeStamp, "vertices":$totalV,"edges":$totalE,"max flow":$maxFlow,"busy roads":$busyEdgeArray,"viewTime":$viewCompleteTime,"concatTime":${System.currentTimeMillis() - startTime}}"""
      Utils.writeLines(output_file, text, "")
      println(text)
    } catch {
      case e: UnsupportedOperationException => println(s"No activity for  view at $timeStamp")
    }
  }
}
