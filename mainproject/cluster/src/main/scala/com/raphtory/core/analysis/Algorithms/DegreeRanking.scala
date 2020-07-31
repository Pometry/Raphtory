package com.raphtory.core.analysis.Algorithms

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.utils.Utils

import scala.collection.mutable.ArrayBuffer

class DegreeRanking(args:Array[String]) extends Analyser(args){
  object sortOrdering extends Ordering[Int] {
    def compare(key1: Int, key2: Int) = key2.compareTo(key1)
  }
  override def analyse(): Unit = {}
  override def setup(): Unit   = {}
  override def returnResults(): Any = {
    val degree = view.getVertices().map { vertex =>
      val outDegree = vertex.getOutEdges.size
      val inDegree  = vertex.getIncEdges.size
      (vertex.ID, outDegree, inDegree)
    }
    val totalV   = degree.size
    val totalOut = degree.map(x => x._2).sum
    val totalIn  = degree.map(x => x._3).sum
    val topUsers = degree.toArray.sortBy(x => x._3)(sortOrdering).take(20)
    (totalV, totalOut, totalIn, topUsers)
  }

  override def defineMaxSteps(): Int = 1

  override def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = {
    val endResults  = results.asInstanceOf[ArrayBuffer[(Int, Int, Int, Array[(Int, Int, Int)])]]
    //    var output_file = System.getenv().getOrDefault("GAB_PROJECT_OUTPUT", "/app/defout.csv").trim
    val startTime   = System.currentTimeMillis()
    val totalVert   = endResults.map(x => x._1).sum
    val totalEdge   = endResults.map(x => x._3).sum

    val degree =
      try totalEdge.toDouble / totalVert.toDouble
      catch { case e: ArithmeticException => 0 }
    var bestUserArray = "["
    val bestUsers = endResults
      .map(x => x._4)
      .flatten
      .sortBy(x => x._3)(sortOrdering)
      .take(20)
      .map(x => s"""{"id":${x._1},"indegree":${x._3},"outdegree":${x._2}}""")
      .foreach(x => bestUserArray += x + ",")
    bestUserArray = if (bestUserArray.length > 1) bestUserArray.dropRight(1) + "]" else bestUserArray + "]"
    val text =
      s"""{"time":$timeStamp,"vertices":$totalVert,"edges":$totalEdge,"degree":$degree,"bestusers":$bestUserArray,"viewTime":$viewCompleteTime,"concatTime":${System
        .currentTimeMillis() - startTime}},"""
    //    Utils.writeLines(output_file, text, "{\"views\":[")
    //println(text)
    publishData(text)
  }

  override def processWindowResults(
      results: ArrayBuffer[Any],
      timestamp: Long,
      windowSize: Long,
      viewCompleteTime: Long
  ): Unit = {
    val endResults  = results.asInstanceOf[ArrayBuffer[(Int, Int, Int, Array[(Int, Int, Int)])]]
//    var output_file = System.getenv().getOrDefault("GAB_PROJECT_OUTPUT", "/app/defout.csv").trim
    val startTime   = System.currentTimeMillis()
    val totalVert   = endResults.map(x => x._1).sum
    val totalEdge   = endResults.map(x => x._3).sum

    val degree =
      try totalEdge.toDouble / totalVert.toDouble
      catch { case e: ArithmeticException => 0 }
    var bestUserArray = "["
    val bestUsers = endResults
      .map(x => x._4)
      .flatten
      .sortBy(x => x._3)(sortOrdering)
      .take(20)
      .map(x => s"""{"id":${x._1},"indegree":${x._3},"outdegree":${x._2}}""")
      .foreach(x => bestUserArray += x + ",")
    bestUserArray = if (bestUserArray.length > 1) bestUserArray.dropRight(1) + "]" else bestUserArray + "]"
    val text =
      s"""{"time":$timestamp,"windowsize":$windowSize,"vertices":$totalVert,"edges":$totalEdge,"degree":$degree,"bestusers":$bestUserArray,"viewTime":$viewCompleteTime,"concatTime":${System
        .currentTimeMillis() - startTime}},"""
//    Utils.writeLines(output_file, text, "{\"views\":[")
    println(text)
    publishData(text)
  }

}
