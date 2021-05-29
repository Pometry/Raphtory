package com.raphtory.algorithms

import com.raphtory.core.analysis.api.Analyser

import scala.collection.mutable.ArrayBuffer

object DegreeBasic{
  def apply() = new DegreeBasic(Array())
}

class DegreeBasic(args:Array[String]) extends Analyser[(Int, Int, Int, Array[(Int, Int, Int)])](args){
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

  override def extractResults(results: List[(Int, Int, Int, Array[(Int, Int, Int)])]): Map[String, Any] = {
    val endResults  = results
  //  val output_file = System.getenv().getOrDefault("PROJECT_OUTPUT", "/app/defout.csv").trim
    val totalVert   = endResults.map(x => x._1).sum
    val totalEdge   = endResults.map(x => x._3).sum

    val degree =
      try totalEdge.toDouble / totalVert.toDouble
      catch {
        case e: ArithmeticException => 0
      }

val startTime   = System.currentTimeMillis()
    val text = s"""{"vertices":$totalVert,"edges":$totalEdge,"degree":$degree}"""
    var output_folder = System.getenv().getOrDefault("OUTPUT_FOLDER", "/app").trim
    var output_file = output_folder + "/" + System.getenv().getOrDefault("OUTPUT_FILE","DegreeBasic.json").trim
   // writeLines(output_file, text, "")
    println(text)
    //publishData(text)
    Map[String,Any]()
  }

}
