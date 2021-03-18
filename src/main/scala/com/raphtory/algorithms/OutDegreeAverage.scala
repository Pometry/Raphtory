package com.raphtory.algorithms

import com.raphtory.api.Analyser

import scala.collection.mutable.ArrayBuffer

class OutDegreeAverage(args:Array[String]) extends Analyser(args){

  override def analyse(): Unit = {}
  override def setup(): Unit   = {}
  override def returnResults(): Any = {
    val outedges = view.getVertices().map { vertex =>vertex.getOutEdges.size}.filter(x=>x>0)
    val degree = outedges.sum
    val totalV   = outedges.size
    (totalV, degree)
  }

  override def defineMaxSteps(): Int = 1

  override def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = {
    val endResults  = results.asInstanceOf[ArrayBuffer[(Int, Int)]]

    val totalVert   = endResults.map(x => x._1).sum
    val totalEdge   = endResults.map(x => x._2).sum

    val degree =
      try totalEdge.toDouble / totalVert.toDouble
      catch { case e: ArithmeticException => 0 }

    val text =
      s"""{"time":$timeStamp,"vertices":$totalVert,"edges":$totalEdge,"degree":$degree}"""
    //    writeLines(output_file, text, "{\"views\":[")
    println(text)
    publishData(text)
  }

  override def processWindowResults(
                                     results: ArrayBuffer[Any],
                                     timestamp: Long,
                                     windowSize: Long,
                                     viewCompleteTime: Long
                                   ): Unit = {

    val endResults  = results.asInstanceOf[ArrayBuffer[(Int, Int)]]
    val totalVert   = endResults.map(x => x._1).sum
    val totalEdge   = endResults.map(x => x._2).sum

    val degree =
      try totalEdge.toDouble / totalVert.toDouble
      catch { case e: ArithmeticException => 0 }
    val text =
      s"""{"time":$timestamp,"windowsize":$windowSize,"vertices":$totalVert,"edges":$totalEdge,"degree":$degree}"""
    println(text)
    publishData(text)
  }

}
