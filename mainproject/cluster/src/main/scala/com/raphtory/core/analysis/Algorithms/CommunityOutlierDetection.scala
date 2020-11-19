package com.raphtory.core.analysis.Algorithms

import com.raphtory.core.analysis.API.entityVisitors.VertexVisitor
import com.raphtory.core.utils.Utils

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.immutable

class CommunityOutlierDetection(args:Array[String]) extends LPA(args) {

  override def doSomething(v: VertexVisitor, neighborLabels: Array[Long]): Unit = {
      val vlabel = neighborLabels(neighborLabels.length-1) //TODO: double check vlabel \in neiLabs
      val outlierScore = 1 - (neighborLabels.count(_==vlabel)/neighborLabels.length.toFloat)
      v.setState("outlierscore", outlierScore)
  }

  def motifCounting(v: VertexVisitor, mType: Int): Float = {
    var inc = v.getIncEdges.flatMap(e=> e.getHistory().keys.toSet).toArray.sorted //TODO modify to include a delta period
    var outg = v.getOutEdges.flatMap(e=> e.getHistory().keys.toSet).toArray.sorted
    val total = (inc.length * outg.length)-(inc.toSet & outg.toSet).size
    mType match {
      case 1 => {
        var cnt = 0
        inc = inc.filter(_ < outg.last)
        inc.foreach{t=>
          outg = outg.filter(_>t)
          cnt+=outg.length
        }
        cnt / total
      }
      case _ => -1
    }
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
