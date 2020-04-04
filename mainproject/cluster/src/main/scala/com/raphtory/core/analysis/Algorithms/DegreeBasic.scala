package com.raphtory.core.analysis.Algorithms

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.utils.Utils

import scala.collection.mutable.ArrayBuffer

class DegreeBasic extends Analyser {
  object sortOrdering extends Ordering[Int] {
    def compare(key1: Int, key2: Int) = key2.compareTo(key1)
  }
  override def analyse(): Unit = {}

  override def setup(): Unit = {}

  override def returnResults(): Any = {
    val degree = proxy.getVerticesSet().map { vert =>
      val vertex    = proxy.getVertex(vert._2)
      val outDegree = vertex.getOutgoingNeighbors.size
      val inDegree  = vertex.getIngoingNeighbors.size
      (vert._1, outDegree, inDegree)
    }
    val totalV   = degree.size
    val totalOut = degree.map(x => x._2).sum
    val totalIn  = degree.map(x => x._3).sum
    val topUsers = degree.toArray.sortBy(x => x._3)(sortOrdering).take(20)
    (totalV, totalOut, totalIn, topUsers)
  }

  override def defineMaxSteps(): Int = 1

  override def processResults(results: ArrayBuffer[Any], timestamp: Long, viewCompleteTime: Long): Unit = {
    val endResults  = results.asInstanceOf[ArrayBuffer[(Int, Int, Int, Array[(Int, Int, Int)])]]
    val output_file = System.getenv().getOrDefault("PROJECT_OUTPUT", "/app/defout.csv").trim
    val totalVert   = endResults.map(x => x._1).sum
    val totalEdge   = endResults.map(x => x._3).sum

    val degree =
      try totalEdge.toDouble / totalVert.toDouble
      catch {
        case e: ArithmeticException => 0
      }
    val text = s"""$timestamp,$totalVert,$totalEdge,$degree"""
    Utils.writeLines(output_file, text, "")
    println(text)
  }

  override def processWindowResults(
      results: ArrayBuffer[Any],
      timestamp: Long,
      windowSize: Long,
      viewCompleteTime: Long
  ): Unit = {
    val endResults  = results.asInstanceOf[ArrayBuffer[(Int, Int, Int, Array[(Int, Int, Int)])]]
    var output_file = System.getenv().getOrDefault("PROJECT_OUTPUT", "/app/defout.csv").trim
    val totalVert   = endResults.map(x => x._1).sum
    val totalEdge   = endResults.map(x => x._3).sum
    val degree =
      try totalEdge.toDouble / totalVert.toDouble
      catch {
        case e: ArithmeticException => 0
      }
    val text = s"""$timestamp,$windowSize,$totalVert,$totalEdge,$degree"""
    Utils.writeLines(output_file, text, "")
   // println(text)
  }

  override def processBatchWindowResults(
      results: ArrayBuffer[Any],
      timestamp: Long,
      windowSet: Array[Long],
      viewCompleteTime: Long
  ): Unit =
    for (i <- results.indices) {
      val window     = results(i).asInstanceOf[ArrayBuffer[Any]]
      val windowSize = windowSet(i)
      processWindowResults(window, timestamp, windowSize, viewCompleteTime)
    }
}
