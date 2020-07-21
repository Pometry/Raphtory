package com.raphtory.core.analysis.Algorithms

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.utils.Utils

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.immutable
class ConnectedComponents(args:Array[String]) extends Analyser(args){

  override def setup(): Unit =
    view.getVertices().foreach { vertex =>
      vertex.setState("cclabel", vertex.ID)
      vertex.messageAllNeighbours(vertex.ID)
    }

  override def analyse(): Unit =
    view.getMessagedVertices().foreach { vertex =>
      val label  = vertex.messageQueue[Long].min
      if (label < vertex.getOrSetState[Long]("cclabel", label)) {
        vertex.setState("cclabel", label)
        vertex messageAllNeighbours label
      }
      else
        vertex.voteToHalt()
    }

  override def returnResults(): Any =
    view.getVertices()
      .map(vertex => vertex.getState[Long]("cclabel"))
      .groupBy(f => f)
      .map(f => (f._1, f._2.size))

  override def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = {
    val endResults  = results.asInstanceOf[ArrayBuffer[immutable.ParHashMap[Long, Int]]]
    try {
      val grouped                  = endResults.flatten.groupBy(f => f._1).mapValues(x => x.map(_._2).sum)
      val groupedNonIslands        = grouped.filter(x => x._2 > 1)
      val biggest                  = grouped.maxBy(_._2)._2
      val total                    = grouped.size
      val totalWithoutIslands      = groupedNonIslands.size
      val totalIslands             = total - totalWithoutIslands
      val proportion               = biggest.toFloat / grouped.map(x => x._2).sum
      val proportionWithoutIslands = biggest.toFloat / groupedNonIslands.map(x => x._2).sum
      val totalGT2                 = grouped.filter(x => x._2 > 2).size
      val text = s"""{"time":$timeStamp,"biggest":$biggest,"total":$total,"totalWithoutIslands":$totalWithoutIslands,"totalIslands":$totalIslands,"proportion":$proportion,"proportionWithoutIslands":$proportionWithoutIslands,"clustersGT2":$totalGT2,"viewTime":$viewCompleteTime},"""
      println(text)
      publishData(text)
    } catch {
      case e: UnsupportedOperationException => println(s"No activity for  view at $timeStamp")
    }
  }

  override def processViewResults(results: ArrayBuffer[Any], timestamp: Long, viewCompleteTime: Long): Unit = {
    val endResults  = results.asInstanceOf[ArrayBuffer[immutable.ParHashMap[Long, Int]]]
    try {
      val grouped                  = endResults.flatten.groupBy(f => f._1).mapValues(x => x.map(_._2).sum)
      val groupedNonIslands        = grouped.filter(x => x._2 > 1)
      val biggest                  = grouped.maxBy(_._2)._2
      val total                    = grouped.size
      val totalWithoutIslands      = groupedNonIslands.size
      val totalIslands             = total - totalWithoutIslands
      val proportion               = biggest.toFloat / grouped.map(x => x._2).sum
      val proportionWithoutIslands = biggest.toFloat / groupedNonIslands.map(x => x._2).sum
      val totalGT2                 = grouped.filter(x => x._2 > 2).size
      val text = s"""{"time":$timestamp,"biggest":$biggest,"total":$total,"totalWithoutIslands":$totalWithoutIslands,"totalIslands":$totalIslands,"proportion":$proportion,"proportionWithoutIslands":$proportionWithoutIslands,"clustersGT2":$totalGT2},"""
      println(text)
      publishData(text)
    } catch {
      case e: UnsupportedOperationException => println(s"No activity for  view at $timestamp")
    }
  }

  override def processWindowResults(results: ArrayBuffer[Any], timestamp: Long, windowSize: Long, viewCompleteTime: Long): Unit = {
    val endResults  = results.asInstanceOf[ArrayBuffer[immutable.ParHashMap[Long, Int]]]
    val startTime   = System.currentTimeMillis()
    try {
      val grouped                  = endResults.flatten.groupBy(f => f._1).mapValues(x => x.map(_._2).sum)
      val groupedNonIslands        = grouped.filter(x => x._2 > 1)
      val biggest                  = grouped.maxBy(_._2)._2
      val total                    = grouped.size
      val totalWithoutIslands      = groupedNonIslands.size
      val totalIslands             = total - totalWithoutIslands
      val proportion               = biggest.toFloat / grouped.map(x => x._2).sum
      val proportionWithoutIslands = biggest.toFloat / groupedNonIslands.map(x => x._2).sum
      val totalGT2                 = grouped.filter(x => x._2 > 2).size
      val text =
        s"""{"time":$timestamp,"windowsize":$windowSize,"biggest":$biggest,"total":$total,"totalWithoutIslands":$totalWithoutIslands,"totalIslands":$totalIslands,"proportion":$proportion,"proportionWithoutIslands":$proportionWithoutIslands,"clustersGT2":$totalGT2,"viewTime":$viewCompleteTime,"concatTime":${System
          .currentTimeMillis() - startTime}},"""
      //Utils.writeLines(output_file, text, "{\"views\":[")
      println(text)
      publishData(text)
    } catch {
      case e: UnsupportedOperationException => println(s"No activity for  view at $timestamp with window $windowSize")
    }
  }

  override def processBatchWindowResults(results: ArrayBuffer[Any], timestamp: Long, windowSet: Array[Long], viewCompleteTime: Long): Unit = {
    val endResults  = results.asInstanceOf[ArrayBuffer[ArrayBuffer[immutable.ParHashMap[Long, Int]]]]
    for (i <- endResults.indices) {
      val window     = endResults(i)
      val windowSize = windowSet(i)
      try {
        val grouped                  = window.flatten.groupBy(f => f._1).mapValues(x => x.map(_._2).sum)
        val groupedNonIslands        = grouped.filter(x => x._2 > 1)
        val biggest                  = grouped.maxBy(_._2)._2
        val total                    = grouped.size
        val totalWithoutIslands      = groupedNonIslands.size
        val totalIslands             = total - totalWithoutIslands
        val proportion               = biggest.toFloat / grouped.map(x => x._2).sum
        val proportionWithoutIslands = biggest.toFloat / groupedNonIslands.map(x => x._2).sum
        val totalGT2                 = grouped.filter(x => x._2 > 2).size
        val text = s"""{"time":$timestamp,"windowsize":$windowSize,"biggest":$biggest,"total":$total,"totalWithoutIslands":$totalWithoutIslands,"totalIslands":$totalIslands,"proportion":$proportion,"proportionWithoutIslands":$proportionWithoutIslands,"clustersGT2":$totalGT2,"viewTime":$viewCompleteTime},"""
        println(text)
        publishData(text)
      } catch {
        case e: UnsupportedOperationException => println(s"No activity for  view at $timestamp with window $windowSize")
      }
    }

  }

  override def defineMaxSteps(): Int = 100

}
