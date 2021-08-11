package com.raphtory.algorithms

import com.raphtory.core.analysis.api.Analyser

import scala.collection.mutable.ArrayBuffer

object DegreeBasic{
  def apply() = new DegreeBasic(Array())
}

class DegreeBasic(args:Array[String]) extends Analyser[(Int, Int, Int, Array[(Long, Int, Int)])](args){
  object sortOrdering extends Ordering[Int] {
    def compare(key1: Int, key2: Int): Int = key2.compareTo(key1)
  }
  override def analyse(): Unit = {}

  override def setup(): Unit = {}

  override def returnResults(): (Int, Int, Int, Array[(Long, Int, Int)]) = {
    val degree = view.getVertices().map { vertex =>
      val outDegree = vertex.getOutEdges.size
      val inDegree  = vertex.getIncEdges.size
      (vertex.ID(), outDegree, inDegree)
    }
    val totalV   = degree.size
    val totalOut = degree.map(x => x._2).sum
    val totalIn  = degree.map(x => x._3).sum
    val topUsers = degree.toArray.sortBy(x => x._3)(sortOrdering).take(20)
    (totalV, totalOut, totalIn, topUsers)
  }

  override def defineMaxSteps(): Int = 1

  override def extractResults(results: List[(Int, Int, Int, Array[(Long, Int, Int)])]): Map[String, Any] = {
    val endResults  = results
    val totalVert   = endResults.map(x => x._1).sum
    val totalEdge   = endResults.map(x => x._3).sum

    val degree =
      try totalEdge.toDouble / totalVert.toDouble
      catch {
        case e: ArithmeticException => 0
      }
    Map[String,Any]("vertices"->totalVert,"edges"->totalEdge, "degree"->degree)
  }

}
