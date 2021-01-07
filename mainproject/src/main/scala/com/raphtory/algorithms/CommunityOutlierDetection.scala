package com.raphtory.algorithms

import com.raphtory.core.model.analysis.entityVisitors.VertexVisitor

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.immutable

class CommunityOutlierDetection(args:Array[String]) extends LPA(args) {
  val thr: Double = System.getenv().getOrDefault("OUTLIER_DETECTION_THRESHOLD", "0.1").trim.toDouble
  val topnum: Int = System.getenv().getOrDefault("OUTLIER_DETECTION_NUMBER", "100").trim.toInt // TODO: add this as arguments

  override def doSomething(v: VertexVisitor, neighborLabels: Array[Long]): Unit = {
      val vlabel = v.getState[Long]("lpalabel")
      val outlierScore = 1 - (neighborLabels.count(_==vlabel)/neighborLabels.length.toDouble)
      v.setState("outlierscore", outlierScore)
  }

  override def returnResults(): Any =
    view.getVertices()
      .map(vertex => (vertex.ID(), vertex.getState[Double]("outlierscore")))

  override def processResults(results: ArrayBuffer[Any], timestamp: Long, viewCompleteTime: Long): Unit = {
    val endResults = results.asInstanceOf[ArrayBuffer[immutable.ParHashMap[Long, Double]]].flatten
    val outliers = endResults.filter(_._2 >= thr)
    val sorted = outliers.sortBy(- _._2)
    val sortedstr = sorted.map(x => s""""${x._1}":${x._2}""")
    val top = sorted.map(_._1).take(5)
    val total = outliers.length
    val proportion = total/endResults.length.toDouble
    val text = s"""{"time":$timestamp,"total":$total,"top5":[${top.mkString(",")}],"outliers":{${sortedstr.take(topnum).mkString(",")}},"proportion":$proportion,"viewTime":$viewCompleteTime}"""
//    writeLines(output_file, text, "{\"views\":[")
    println(text)
  }

  override def processWindowResults(results: ArrayBuffer[Any], timestamp: Long, windowSize: Long, viewCompleteTime: Long): Unit = {
    val endResults = results.asInstanceOf[ArrayBuffer[immutable.ParHashMap[Long, Double]]].flatten
    val outliers = endResults.filter(_._2 >= thr)
    val sorted = outliers.sortBy(- _._2)
    val sortedstr = sorted.map(x => s""""${x._1}":${x._2}""")
    val top = sorted.map(_._1).take(5)
    val total = outliers.length
    val proportion = total/endResults.length.toDouble
    val text = s"""{"time":$timestamp,"windowsize":$windowSize,"total":$total,"top5":[${top.mkString(",")}],"outliers":{${sortedstr.take(topnum).mkString(",")}},"proportion":$proportion,"viewTime":$viewCompleteTime},"""
//    writeLines(output_file, text, "{\"views\":[")
    println(text)
  }
}
