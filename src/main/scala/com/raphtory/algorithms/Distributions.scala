package com.raphtory.algorithms

import com.raphtory.core.analysis.api.Analyser

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.immutable


/** Warning: will generate large amount of data, best to run on small numbers of snapshots */

class Distributions(args:Array[String]) extends Analyser[Any](args){

  object sortOrdering extends Ordering[Int] {
    def compare(key1: Int, key2: Int) = key2.compareTo(key1)
  }

  override def analyse(): Unit = {}

  override def setup(): Unit = {}

  override def returnResults(): Any = {
    val degreeDist = view.getVertices().map{
      vertex => vertex.getOutEdges.size + vertex.getIncEdges.size
    } .groupBy(f => f)
      .map(f => (f._1, f._2.size))
    val weightDist = view.getVertices().map{
      vertex => vertex.getOutEdges.map(e => e.getHistory().size).sum + vertex.getIncEdges.map(e => e.getHistory().size).sum
    }.groupBy(f => f)
      .map(f => (f._1, f._2.size))
    val edgeWeights = view.getVertices().map{
      vertex => vertex.getOutEdges.map(e => ((List(e.src(),e.dst()).min, List(e.src(),e.dst()).max), e.getHistory().size))
    }.flatten
      .groupBy(f => f._1)
      .map (f => (f._1, f._2.map(_._2).sum))
    (degreeDist, weightDist, edgeWeights)
  }

  override def defineMaxSteps(): Int = 1

  override def extractResults(results: List[Any]): Map[String,Any]  = {
    var output_folder = System.getenv().getOrDefault("OUTPUT_FOLDER", "/app").trim
    var output_file = output_folder + "/" + System.getenv().getOrDefault("OUTPUT_FILE","Distributions.json").trim
    val er = extractData(results)
    val startTime   = System.currentTimeMillis()

    val degDistArr = er.degDist
    val weightDistArr = er.weightDist
    val edgeDistArr = er.edgeWeights

    val text = s"""{"degDist":$degDistArr,"weightDist":$weightDistArr, "edgeDist":$edgeDistArr}"""
    println(text)
    publishData(text)
    Map[String,Any]()
  }

  def extractData(results: List[Any]): extractedData = {
    val endResults = results.asInstanceOf[ArrayBuffer[(immutable.ParHashMap[Int, Int], immutable.ParHashMap[Int, Int], immutable.ParHashMap[(Long,Long), Int])]]
    val degreeDist = endResults.map(x => x._1)
      .flatten
      .groupBy(f => f._1)
      .map(x => (x._1, x._2.map(_._2).sum))
      .map(x => s"""{"degree":${x._1},"freq":${x._2}}""")
      .mkString("[",",","]")
    val weightDist = endResults.map(x => x._2)
      .flatten
      .groupBy(f => f._1)
      .map(x => (x._1, x._2.map(_._2).sum))
      .map(x => s"""{"weight":${x._1},"freq":${x._2}}""")
      .mkString("[",",","]")
    val edgeWeights = endResults.map(x => x._3)
      .flatten
      .groupBy(f => f._1)
      .map(x => x._2.map(_._2).sum)
      .groupBy(f => f)
      .map(f => (f._1, f._2.size))
      .map(x => s"""{"eweight":${x._1},"freq":${x._2}}""")
      .mkString("[",",","]")
    extractedData(degreeDist, weightDist, edgeWeights)
  }

  case class extractedData(degDist:String, weightDist:String, edgeWeights:String)
}
