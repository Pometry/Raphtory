package com.raphtory.algorithms

import com.raphtory.core.model.analysis.entityVisitors.VertexVisitor
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.immutable
import scala.reflect.io.Path

object CommunityOutlierDetection {
  def apply(args:Array[String]): CommunityOutlierDetection = new CommunityOutlierDetection(args)
}

class CommunityOutlierDetection(args: Array[String]) extends LPA(args) {
  //args = [top output, edge property, maxIter, threshold]
  val topnum: Int           = if (arg.length == 0) 0 else arg.head.toInt
  val thr: Double           = if (arg.length < 4) 0.0 else arg(3).toDouble

  override def doSomething(v: VertexVisitor, neighborLabels: Array[Long]): Unit = {
    val vlabel       = v.getState[Long]("lpalabel")
    val outlierScore = 1 - (neighborLabels.count(_ == vlabel) / neighborLabels.length.toDouble)
    v.setState("outlierscore", outlierScore)
  }

  override def returnResults(): Any =
    view
      .getVertices()
      .filter(v => v.Type() == nodeType)
      .map(vertex => (vertex.ID(), vertex.getOrSetState[Double]("outlierscore", -1.0))) //TODO: IM: temp fix -- to be removed later

  override def processResults(results: ArrayBuffer[Any], timestamp: Long, viewCompleteTime: Long): Unit = {
    val endResults = results.asInstanceOf[ArrayBuffer[immutable.ParHashMap[Long, Double]]].flatten
    val outliers   = endResults.filter(_._2 >= thr)
    val sorted     = outliers.sortBy(-_._2)
    val sortedstr  = sorted.map(x => s""""${x._1}":${x._2}""")
    val top        = sorted.map(_._1).take(5)
    val total      = outliers.length
    val out        = if (topnum == 0) sortedstr else sortedstr.take(topnum)
    val text = s"""{"time":$timestamp,"total":$total,"top5":[${top.mkString(",")}],"outliers":{${out
      .mkString(",")}},"viewTime":$viewCompleteTime}"""
//    Path(output_file).createFile().appendAll(text + "\n")
        println(text)
  }

  override def processWindowResults(
      results: ArrayBuffer[Any],
      timestamp: Long,
      windowSize: Long,
      viewCompleteTime: Long
  ): Unit = {
    val endResults = results.asInstanceOf[ArrayBuffer[immutable.ParHashMap[Long, Double]]].flatten
    val outliers   = endResults.filter(_._2 >= thr)
    val sorted     = outliers.sortBy(-_._2)
    val sortedstr  = sorted.map(x => s""""${x._1}":${x._2}""")
    val top        = sorted.map(_._1).take(5)
    val total      = outliers.length
    val out        = if (topnum == 0) sortedstr else sortedstr.take(topnum)
    val text = s"""{"time":$timestamp,"windowsize":$windowSize,"total":$total,"top5":[${top
      .mkString(",")}],"outliers":{${out.mkString(",")}},"viewTime":$viewCompleteTime}"""
//    Path(output_file).createFile().appendAll(text + "\n")
    println(text)
  }
}
