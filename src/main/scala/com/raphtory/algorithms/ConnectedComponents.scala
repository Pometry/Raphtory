package com.raphtory.algorithms

import com.raphtory.core.analysis.api.Analyser

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.immutable

object ConnectedComponents {
  def apply():ConnectedComponents = new ConnectedComponents(Array())
}

class ConnectedComponents(args:Array[String]) extends Analyser[List[(Long, Int)]](args){

  override def setup(): Unit =
    view.getVertices().foreach { vertex =>
      vertex.setState("cclabel", vertex.ID)
      vertex.messageAllNeighbours(vertex.ID)
    }

  override def analyse(): Unit =
    view.getMessagedVertices().foreach { vertex =>
      val label  = vertex.messageQueue[Long].min
      if (label < vertex.getState[Long]("cclabel")) {
        vertex.setState("cclabel", label)
        vertex messageAllNeighbours label
      }
      else
        vertex.voteToHalt()
    }

  override def returnResults(): Any = {
    val x = view.getVertices()
      .map(vertex => vertex.getState[Long]("cclabel"))
      .groupBy(f => f)
      .map(f => (f._1, f._2.size)).toList
  }

  override def extractResults(results: List[List[(Long, Int)]]): Map[String, Any] = {
    val er = extractData(results)
    Map[String,Any]("top5"-> er.top5,"total"->er.total,"totalIslands"->er.totalIslands,"proportion"->er.proportion,"clustersGT2"->er.totalGT2)
  }


  def extractData(results:List[List[(Long, Int)]]):extractedData ={
    val endResults = results
    try {
      val grouped = endResults.flatten.groupBy(f => f._1).mapValues(x => x.map(_._2).sum)
      val groupedNonIslands = grouped.filter(x => x._2 > 1)
      val biggest = grouped.maxBy(_._2)._2
      val sorted = groupedNonIslands.toArray.sortBy(_._2)(sortOrdering).map(x=>x._2)
      val top5 = if(sorted.length<=5) sorted else sorted.take(5)
      val total = grouped.size
      val totalWithoutIslands = groupedNonIslands.size
      val totalIslands = total - totalWithoutIslands
      val proportion = biggest.toFloat / grouped.values.sum
      val totalGT2 = grouped.count(x => x._2 > 2)
      extractedData(top5,total,totalIslands,proportion,totalGT2)
    } catch {
      case e: UnsupportedOperationException => extractedData(Array(0),0,0,0,0)
    }
  }

  override def defineMaxSteps(): Int = 100

}
object sortOrdering extends Ordering[Int] {
  def compare(key1: Int, key2: Int) = key2.compareTo(key1)
}
case class extractedData(top5:Array[Int],total:Int,totalIslands:Int,proportion:Float,totalGT2:Int)

