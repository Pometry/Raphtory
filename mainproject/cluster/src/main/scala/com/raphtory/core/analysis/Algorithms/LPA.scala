package com.raphtory.core.analysis.Algorithms

import com.raphtory.core.analysis.API.Analyser

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.immutable
import scala.collection.parallel.mutable.{ParArray}

class LPA(args:Array[String]) extends Analyser(args){
  override def setup(): Unit = {
    view.getVertices().foreach { vertex =>
      vertex.setState("lpalabel", vertex.ID())
      vertex messageAllNeighbours vertex.ID()
    }
  }

  override def analyse(): Unit = {
    view.getMessagedVertices().foreach { vertex =>
      val vlabel = vertex.getState[Long]("lpalabel")
      val gp = vertex.messageQueue[Long]
      gp.append(vlabel)
      val newLabel = labelProbability(gp.groupBy(identity))
      if (newLabel == vlabel) {
        vertex.voteToHalt()
      }else {
        vertex.setState("lpalabel", newLabel)
      }
      vertex messageAllNeighbours newLabel
    }
  }

  def labelProbability(gp: Map[Long, ArrayBuffer[Long]]): Long ={
//    label probability function to get rid of oscillation phenomena of synchronous LPA from [1]
//    [1] https://doi.org/10.1016/j.neucom.2014.04.084

    val f = gp.map{lab =>  Math.pow(lab._2.size, 2)}
    val p = f.toArray.map(i => i/f.toArray.sum)
    val gpp = gp.keys zip p
    gpp.filter(x=>x._2 == gpp.maxBy(_._2)._2).maxBy(_._1)._1
  }

  override def returnResults(): Any =
    view.getVertices()
      .map(vertex => (vertex.getState[Long]("lpalabel"), vertex.ID()))
      .groupBy(f => f._1)
      .map(f => (f._1, f._2.map(_._2)))

  override def processResults(results: ArrayBuffer[Any], timestamp: Long, viewCompleteTime: Long): Unit = {
    val er = extractData(results)
    val commtxt = er.communities.map{x=> s"""[${x.mkString(",")}]"""}
    val text = s"""{"time":$timestamp,"top5":[${er.top5.mkString(",")}],"total":${er.total},"totalIslands":${er.totalIslands},"proportion":${er.proportion},"clustersGT2":${er.totalGT2}, "communities":[${commtxt.mkString(",")}],"viewTime":$viewCompleteTime}"""
    println(text)
    publishData(text)
  }

  override def processWindowResults(results: ArrayBuffer[Any], timestamp: Long, windowSize: Long, viewCompleteTime: Long): Unit = {
    val er = extractData(results)
    val commtxt = er.communities.map{x=> s"""[${x.mkString(",")}]}"""}
    val text = s"""{"time":$timestamp,"windowsize":$windowSize,"top5":[${er.top5.mkString(",")}],"total":${er.total},"totalIslands":${er.totalIslands},"proportion":${er.proportion},"clustersGT2":${er.totalGT2}, "communities": [${commtxt.mkString(",")}],"viewTime":$viewCompleteTime}"""
    println(text)
    publishData(text)

  }

  def extractData(results:ArrayBuffer[Any]):fd ={
    val endResults = results.asInstanceOf[ArrayBuffer[immutable.ParHashMap[Long,  ParArray[Long]]]]
    try {
      val grouped = endResults.flatten.groupBy(f => f._1).mapValues(x => x.flatMap(_._2))
      val groupedNonIslands = grouped.filter(x => x._2.size > 1)
      val biggest = grouped.maxBy(_._2.size)._2.size
      val sorted = groupedNonIslands.toArray.sortBy(_._2.size)(sortOrdering).map(x=>x._2.size)
      val top5 = if(sorted.length<=10) sorted else sorted.take(10)
      val total = grouped.size
      val totalWithoutIslands = groupedNonIslands.size
      val totalIslands = total - totalWithoutIslands
      val proportion = biggest.toFloat / grouped.map(x => x._2.size).sum
      val totalGT2 = grouped.count(x => x._2.size > 2)
      val communities =  grouped.map(x=>x._2.toArray).toArray
      fd(top5,total,totalIslands,proportion,totalGT2, communities)
    }  catch {
      case e: UnsupportedOperationException => fd(Array(0),0,0,0,0, Array(Array(0)))
    }
  }

  override def defineMaxSteps(): Int = 100

}

case class fd(top5:Array[Int],total:Int,totalIslands:Int,proportion:Float,totalGT2:Int, communities: Array[Array[Long]])

