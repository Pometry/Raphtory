package com.raphtory.core.analysis.Algorithms

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.utils.Utils

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.immutable
class ConnectedComponents(args:Array[String]) extends Analyser(args){

  override def setup(): Unit =
    view.getVertices().foreach { vertex =>
      var min    = vertex.ID
      //math.min(v, vertex.getOutgoingNeighbors.union(vertex.getIngoingNeighbors).min)
      val toSend = vertex.getOrSetCompValue("cclabel", min).asInstanceOf[Long]
      vertex.messageAllNeighbours(toSend)
    }

  override def analyse(): Unit =
    view.getMessagedVertices().foreach { vertex =>
      var label  = vertex.ID
      val queue  = vertex.messageQueue.map(_.asInstanceOf[Long])
      if (queue.nonEmpty) {
        label = queue.min
        vertex.clearQueue
      }
      var currentLabel = vertex.getOrSetCompValue("cclabel", label).asInstanceOf[Long]
      if (label < currentLabel) {
        vertex.setCompValue("cclabel", label)
        vertex messageAllNeighbours label
        currentLabel = label
      } else
        vertex.voteToHalt()
    }

  override def returnResults(): Any =
    view
      .getVertices()
      .map(vertex => vertex.getOrSetCompValue("cclabel", vertex.ID).asInstanceOf[Long])
      .groupBy(f => f)
      .map(f => (f._1, f._2.size))

  override def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = {
    val endResults  = results.asInstanceOf[ArrayBuffer[immutable.ParHashMap[Long, Int]]]
    var output_file = System.getenv().getOrDefault("GAB_PROJECT_OUTPUT", "/opt/docker/output.csv").trim
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
      //todo fix
      val text =
        s"""{"time":$timeStamp,"biggest":$biggest,"total":$total,"totalWithoutIslands":$totalWithoutIslands,"totalIslands":$totalIslands,"proportion":$proportion,"proportionWithoutIslands":$proportionWithoutIslands,"clustersGT2":$totalGT2,"viewTime":$viewCompleteTime,"concatTime":${System
          .currentTimeMillis() - startTime}},"""
      //Utils.writeLines(output_file, text, "{\"views\":[")
      println(text)
      publishData(text)
    } catch {
      case e: UnsupportedOperationException => println(s"No activity for  view at $timeStamp")
    }
  }

  override def processViewResults(results: ArrayBuffer[Any], timestamp: Long, viewCompleteTime: Long): Unit = {
    val endResults  = results.asInstanceOf[ArrayBuffer[immutable.ParHashMap[Long, Int]]]
    var output_file = System.getenv().getOrDefault("GAB_PROJECT_OUTPUT", "/opt/docker/output.csv").trim
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
        s"""{"time":$timestamp,"biggest":$biggest,"total":$total,"totalWithoutIslands":$totalWithoutIslands,"totalIslands":$totalIslands,"proportion":$proportion,"proportionWithoutIslands":$proportionWithoutIslands,"clustersGT2":$totalGT2,"viewTime":$viewCompleteTime,"concatTime":${System
          .currentTimeMillis() - startTime}},"""
      //Utils.writeLines(output_file, text, "{\"views\":[")
      println(text)
      publishData(text)

    } catch {
      case e: UnsupportedOperationException => println(s"No activity for  view at $timestamp")
    }
  }

  override def processWindowResults(
      results: ArrayBuffer[Any],
      timestamp: Long,
      windowSize: Long,
      viewCompleteTime: Long
  ): Unit = {

    val endResults  = results.asInstanceOf[ArrayBuffer[immutable.ParHashMap[Long, Int]]]
    var output_file = System.getenv().getOrDefault("GAB_PROJECT_OUTPUT", "/opt/docker/output.csv").trim
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

  override def processBatchWindowResults(
      results: ArrayBuffer[Any],
      timestamp: Long,
      windowSet: Array[Long],
      viewCompleteTime: Long
  ): Unit = {
    var output_file = System.getenv().getOrDefault("GAB_PROJECT_OUTPUT", "/opt/docker/output.csv").trim
    val endResults  = results.asInstanceOf[ArrayBuffer[ArrayBuffer[immutable.ParHashMap[Long, Int]]]]
    for (i <- endResults.indices) {
      val startTime  = System.currentTimeMillis()
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
        //println(s"$timestamp $windowSize $biggest $total $window")
        val text =
          s"""{"time":$timestamp,"windowsize":$windowSize,"biggest":$biggest,"total":$total,"totalWithoutIslands":$totalWithoutIslands,"totalIslands":$totalIslands,"proportion":$proportion,"proportionWithoutIslands":$proportionWithoutIslands,"clustersGT2":$totalGT2,"viewTime":$viewCompleteTime,"concatTime":${System
            .currentTimeMillis() - startTime}},"""
        //Utils.writeLines(output_file, text, "{\"views\":[")
        println(text)
        publishData(text)
        //println(s"At ${new Date(timestamp)} with a window of ${windowSize / 3600000} hour(s) there were ${} connected components. The biggest being ${}")
      } catch {
        case e: UnsupportedOperationException => println(s"No activity for  view at $timestamp with window $windowSize")
      }
    }

  }

  override def defineMaxSteps(): Int = 100

}
