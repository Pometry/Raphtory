package com.raphtory.algorithms

import com.raphtory.core.model.analysis.entityVisitors.VertexVisitor

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.immutable
import scala.reflect.io.Path

class CommunityOutlierDetection(args:Array[String]) extends LPA(args) {
  val thr: Double = System.getenv().getOrDefault("OUTLIER_DETECTION_THRESHOLD", "0.0").trim.toDouble
  val topnum: Int = System.getenv().getOrDefault("OUTLIER_DETECTION_NUMBER", "-1").trim.toInt // TODO: add this as arguments

  override def doSomething(v: VertexVisitor, neighborLabels: Array[Long]): Unit = {
      val vlabel = v.getState[Long]("lpalabel")
      val outlierScore = 1 - (neighborLabels.count(_==vlabel)/neighborLabels.length.toDouble)
      v.setState("outlierscore", outlierScore)
  }

  override def returnResults(): Any =
    view.getVertices().filter(v=> v.Type()==nodeType)
      .map(vertex => (vertex.ID(), vertex.getOrSetState[Double]("outlierscore", -1.0)))

  override def processResults(results: ArrayBuffer[Any], timestamp: Long, viewCompleteTime: Long): Unit = {
    val endResults = results.asInstanceOf[ArrayBuffer[immutable.ParHashMap[Long, Double]]].flatten
    val outliers = endResults.filter(_._2 >= thr)
    val sorted = outliers.sortBy(- _._2)
    val sortedstr = sorted.map(x => s""""${x._1}":${x._2}""")
    val top = sorted.map(_._1).take(5)
    val total = outliers.length
    val proportion = total/endResults.length.toDouble
    val out = if (topnum == -1) sortedstr else sortedstr.take(topnum)
    val text = s"""{"time":$timestamp,"total":$total,"top5":[${top.mkString(",")}],"outliers":{${out.mkString(",")}},"proportion":$proportion,"viewTime":$viewCompleteTime}"""
    Path(output_file).createFile().appendAll(text + "\n")
    //    println(text)
  }

  override def processWindowResults(results: ArrayBuffer[Any], timestamp: Long, windowSize: Long, viewCompleteTime: Long): Unit = {
    val endResults = results.asInstanceOf[ArrayBuffer[immutable.ParHashMap[Long, Double]]].flatten
    val outliers = endResults.filter(_._2 >= thr)
    val sorted = outliers.sortBy(- _._2)
    val sortedstr = sorted.map(x => s""""${x._1}":${x._2}""")
    val top = sorted.map(_._1).take(5)
    val total = outliers.length
    val proportion = total/endResults.length.toDouble
    val out = if (topnum == -1) sortedstr else sortedstr.take(topnum)
    val text = s"""{"time":$timestamp,"windowsize":$windowSize,"total":$total,"top5":[${top.mkString(",")}],"outliers":{${out.mkString(",")}},"proportion":$proportion,"viewTime":$viewCompleteTime}"""
    Path(output_file).createFile().appendAll(text + "\n")
//    println(text)
  }
}
