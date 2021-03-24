package com.raphtory.dev.algorithms

import com.github.mjakubowski84.parquet4s.ParquetWriter
import com.raphtory.core.analysis.entity.Vertex
import org.apache.parquet.hadoop.ParquetFileWriter

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ParMap

/**
A version of MultiLayerLPA that gives freedom over filering the edges to consider
-- Work specific to the Word Semantics Project
 **/
object MultiLayerLPAparams {
  def apply(args: Array[String]): MultiLayerLPAparams = new MultiLayerLPAparams(args)
}
class MultiLayerLPAparams(args: Array[String]) extends MultiLayerLPA(args) {
  //args = [top, weight, maxiter, start, end, layer-size, omega, theta]
//  val theta: Double = if (arg.length < 8) 0.0 else args(7).toDouble
  val scaled: Boolean = if (arg.length < 8) false else args(7).toBoolean

  override def weightFunction(v: Vertex, ts: Long): ParMap[Long, Double] = {
    var nei_weights =
      (v.getInCEdgesBetween(ts - snapshotSize, ts) ++ v.getOutEdgesBetween(ts - snapshotSize, ts)).map(e =>
        (e.ID(), e.getPropertyValue(weight).getOrElse(1.0).asInstanceOf[Double])
      )
    if (scaled) {
      val scale = scaling(nei_weights.map(_._2).toArray)
      nei_weights = nei_weights.map(x => (x._1, x._2 / scale))

    }
    nei_weights.groupBy(_._1).mapValues(x => x.map(_._2).sum / x.size) // (ID -> Freq)
  }

  def scaling(freq: Array[Double]): Double = math.sqrt(freq.map(math.pow(_, 2)).sum)

  override def processResults(results: ArrayBuffer[Any], timestamp: Long, viewCompleteTime: Long): Unit = {
    val er = extractData(results)
    output_file match {
      case "" =>
        val commtxt = er.communities.map(x => s"""[${x.mkString(",")}]""")
        val text = s"""{"time":$timestamp,"top5":[${er.top5
          .mkString(",")}],"total":${er.total},"totalIslands":${er.totalIslands},""" +
          s""" "communities": [${commtxt.mkString(",")}],""" +
          s"""viewTime":$viewCompleteTime}"""
        println(text)
      case _ =>
        val text = s"""{"time":$timestamp,"top5":[${er.top5
          .mkString(",")}],"total":${er.total},"totalIslands":${er.totalIslands}, "viewTime":$viewCompleteTime}"""
        println(text)
        case class Data(comm: Array[String])
        val writer =
          ParquetWriter.writer[Data](output_file, ParquetWriter.Options(writeMode = ParquetFileWriter.Mode.OVERWRITE))
        try er.communities.foreach(c => writer.write(Data(c.toArray)))
        finally writer.close()
    }
  }
}
