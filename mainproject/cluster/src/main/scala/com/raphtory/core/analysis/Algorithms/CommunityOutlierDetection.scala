package com.raphtory.core.analysis.Algorithms

import com.raphtory.core.analysis.API.entityVisitors.VertexVisitor
import com.raphtory.core.utils.Utils

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.immutable

class CommunityOutlierDetection(args:Array[String]) extends LPA(args) {
  val output_file = System.getenv().getOrDefault("OUTPUT_PATH", "/app/out.json").trim
  override def doSomething(v: VertexVisitor, neighborLabels: Array[Long]): Unit = {
      val vlabel = neighborLabels(neighborLabels.length-1)
      val outlierScore = 1 - (neighborLabels.count(_==vlabel)/neighborLabels.length.toFloat)
      v.setState("outlierscore", outlierScore)
  }

  override def returnResults(): Any =
    view.getVertices()
      .map(vertex => (vertex.ID(), vertex.getState[Float]("outlierscore")))
//      .groupBy(f=> f._2)
//      .map(f => f._2)

  override def processResults(results: ArrayBuffer[Any], timestamp: Long, viewCompleteTime: Long): Unit = {
    val endResults = results.asInstanceOf[ArrayBuffer[immutable.ParHashMap[Long, Float]]].flatten
    val thr = System.getenv().getOrDefault("OUTLIER_DETECTION_THRESHOLD", "0.5").trim.toFloat
    val topnum = System.getenv().getOrDefault("OUTLIER_DETECTION_NUMBER", "100").trim.toInt
    val outliers = endResults.filter(_._2 >= thr)
    val sorted = outliers.sortBy(- _._2)
    val sortedstr = sorted.map(x => s""""${x._1}":${x._2}""")
    val top = sorted.map(_._1).take(5)
    val total = outliers.length
    val proportion = total/endResults.length.toFloat
    val text = s"""{"time":$timestamp,"total":$total,"top5":[${top.mkString(",")}],"outliers":{${sortedstr.take(topnum).mkString(",")}},"proportion":$proportion,"viewTime":$viewCompleteTime}"""
    Utils.writeLines(output_file, text, "{\"views\":[")
    println(text)
    publishData(text)
  }

  override def processWindowResults(results: ArrayBuffer[Any], timestamp: Long, windowSize: Long, viewCompleteTime: Long): Unit = {
    val endResults = results.asInstanceOf[ArrayBuffer[immutable.ParHashMap[Long, Float]]].flatten
    val thr = System.getenv().getOrDefault("OUTLIER_DETECTION_THRESHOLD", "0.5").trim.toFloat
    val topnum = System.getenv().getOrDefault("OUTLIER_DETECTION_NUMBER", "100").trim.toInt
    val outliers = endResults.filter(_._2 >= thr)
    val sorted = outliers.sortBy(- _._2)
    val sortedstr = sorted.map(x => s""""${x._1}":${x._2}""")
    val top = sorted.map(_._1).take(5)
    val total = outliers.length
    val proportion = total/endResults.length.toFloat
    val text = s"""{"time":$timestamp,"windowsize":$windowSize,"total":$total,"top5":[${top.mkString(",")}],"outliers":{${sortedstr.take(topnum).mkString(",")}},"proportion":$proportion,"viewTime":$viewCompleteTime},"""
    Utils.writeLines(output_file, text, "{\"views\":[")
    println(text)
    publishData(text)
  }
}
