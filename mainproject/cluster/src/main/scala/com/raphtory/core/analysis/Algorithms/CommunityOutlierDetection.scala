package com.raphtory.core.analysis.Algorithms

import com.raphtory.core.analysis.API.entityVisitors.VertexVisitor

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.immutable
import scala.collection.parallel.mutable.ParArray

class CommunityOutlierDetection(args:Array[String]) extends LPA(args) {
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
    val outliers = endResults.filter(_._2 >= thr)
    val sorted = outliers.sortBy(- _._2).map(_._1)
    val top = sorted.take(Seq(5, sorted.length).min)
    val total = outliers.length
    val proportion = total/endResults.length.toFloat
    val text = s"""{"time":$timestamp,"total":$total,"top5":[${top.mkString(",")}],"outliers":[${sorted.mkString(",")}],"proportion":$proportion,"viewTime":$viewCompleteTime}"""
    println(text)
    publishData(text)
  }

  override def processWindowResults(results: ArrayBuffer[Any], timestamp: Long, windowSize: Long, viewCompleteTime: Long): Unit = {
    val endResults = results.asInstanceOf[ArrayBuffer[immutable.ParHashMap[Long, Float]]].flatten
    val thr = System.getenv().getOrDefault("OUTLIER_DETECTION_THRESHOLD", "0.5").trim.toFloat
    val outliers = endResults.filter(_._2 >= thr)
    val sorted = outliers.sortBy(- _._2).map(_._1)
    val top = sorted.take(Seq(5, sorted.length).min)
    val total = outliers.length
    val proportion = total/endResults.length.toFloat
    val text = s"""{"time":$timestamp,"windowsize":$windowSize,"total":$total,"top5":[${top.mkString(",")}],"outliers":[${sorted.mkString(",")}],"proportion":$proportion,"viewTime":$viewCompleteTime}"""
    println(text)
    publishData(text)
  }
}
