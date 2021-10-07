package com.raphtory.algorithms.old

import com.raphtory.core.model.algorithm.Analyser

import scala.collection.mutable.ArrayBuffer

class DegreeRanking(args:Array[String]) extends Analyser[Any](args){
  object sortOrdering extends Ordering[Int] {
    def compare(key1: Int, key2: Int) = key2.compareTo(key1)
  }
  var output_file: String = System.getenv().getOrDefault("DR_OUTPUT_PATH", "").trim
  val weighted = false

  override def analyse(): Unit = {}
  override def setup(): Unit   = {}
  override def returnResults(): Any = {
    val degree =
      if (weighted) {
      view.getVertices().map { vertex =>
      val outDegree = vertex.getOutEdges().map{e => e.history().size}.sum
      val inDegree = vertex.getInEdges().map{ e => e.history().size}.sum
      (vertex.ID, outDegree, inDegree)}}
    else {
      view.getVertices().map { vertex =>
        val outDegree = vertex.getOutEdges().size
        val inDegree = vertex.getInEdges().size
        (vertex.ID(), outDegree, inDegree)}
    }

    val totalV   = degree.size
    val totalOut = degree.map(x => x._2).sum
    val totalIn  = degree.map(x => x._3).sum
    val topUsers = degree.toArray.sortBy(x => x._3)(sortOrdering).take(20)
    (totalV, totalOut, totalIn, topUsers.toList)
  }

  override def defineMaxSteps(): Int = 1

  override def extractResults(results: List[Any]): Map[String,Any]  = {
    val endResults  = results.asInstanceOf[ArrayBuffer[(Int, Int, Int, List[(Int, Int, Int)])]]

    val startTime   = System.currentTimeMillis()
    val totalVert   = endResults.map(x => x._1).sum
    val totalEdge   = endResults.map(x => x._3).sum

    val degree =
      try totalEdge.toDouble / totalVert.toDouble
      catch { case e: ArithmeticException => 0 }
    var bestUserArray = "["
    val bestUsers = endResults.flatMap(x => x._4)
      .sortBy(x => x._2)(sortOrdering)
      .take(20)
      .map(x => s"""{"id":${x._1},"indegree":${x._3},"outdegree":${x._2}}""")
      .foreach(x => bestUserArray += x + ",")
    bestUserArray = if (bestUserArray.length > 1) bestUserArray.dropRight(1) + "]" else bestUserArray + "]"

    Map("vertices"->totalVert,"edges"->totalEdge,"degree"->degree,"bestusers"->bestUserArray,"concatTime"->(System
      .currentTimeMillis() - startTime))
  }
}
