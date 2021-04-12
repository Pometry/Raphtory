package com.raphtory.algorithms

import com.raphtory.api.Analyser

import scala.collection.mutable.ArrayBuffer

object DegreeBasic{
  def apply() = new DegreeBasic(Array())
}

class DegreeBasic(args:Array[String]) extends Analyser(args){
  val output_file: String = System.getenv().getOrDefault("DB_OUTPUT_PATH", "").trim
  object sortOrdering extends Ordering[Int] {
    def compare(key1: Int, key2: Int) = key2.compareTo(key1)
  }
  override def analyse(): Unit = {}

  override def setup(): Unit = {}

  override def returnResults(): Any = {
    val degree = view.getVertices().map { vertex =>
      val outDegree = vertex.getOutEdges.size
      val inDegree  = vertex.getIncEdges.size
      (vertex.ID(), outDegree, inDegree)
    }
    val totalV   = degree.size
    val totalOut = degree.map(x => x._2).sum
    val totalIn  = degree.map(x => x._3).sum
//    val topUsers = degree.toArray.sortBy(x => x._3)(sortOrdering).take(20)
    (totalV, totalOut, totalIn)//, topUsers)
  }

  override def defineMaxSteps(): Int = 1

  override def processResults(results: ArrayBuffer[Any], timestamp: Long, viewCompleteTime: Long): Unit = {
    val endResults  = results.asInstanceOf[ArrayBuffer[(Int, Int, Int)]]//, Array[(Int, Int, Int)])]]
    val totalVert   = endResults.map(x => x._1).sum
    val totalEdge   = endResults.map(x => x._3).sum

    val degree =
      try totalEdge.toDouble / totalVert.toDouble
      catch {
        case e: ArithmeticException => 0
      }

    val text = s"""{"time":$timestamp,"vertices":$totalVert,"edges":$totalEdge,"degree":$degree}"""
    writeOut(text, output_file)
  }

  override def processWindowResults(
      results: ArrayBuffer[Any],
      timestamp: Long,
      windowSize: Long,
      viewCompleteTime: Long
  ): Unit = {
    val endResults = results.asInstanceOf[ArrayBuffer[(Int, Int, Int)]]//, Array[(Int, Int, Int)])]]
    val totalVert = endResults.map(x => x._1).sum
    val totalEdge = endResults.map(x => x._3).sum
    val degree =
      try totalEdge.toDouble / totalVert.toDouble
      catch {
        case e: ArithmeticException => 0
      }
    val text = s"""{"time":$timestamp,"windowsize":$windowSize,"vertices":$totalVert,"edges":$totalEdge,"degree":$degree},"""
    writeOut(text, output_file)
  }
}
