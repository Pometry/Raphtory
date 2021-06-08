package com.raphtory.dev.algorithms

import com.raphtory.core.model.analysis.entityVisitors.VertexVisitor
import org.apache.parquet.hadoop.ParquetFileWriter

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.{ParMap, immutable}
import scala.io.Source

/**
A version of MultiLayerLPA that gives freedom over filering the edges to consider
-- Work specific to the Word Semantics Project
 **/
object MultiLayerLPAparams {
  def apply(args: Array[String]): MultiLayerLPAparams = new MultiLayerLPAparams(args)
}
class MultiLayerLPAparams(args: Array[String]) extends MultiLayerLPA(args) {
  val url="https://raphtorydatasets.blob.core.windows.net/top-tier/misc/selnodes.csv"
  val nodesf: String = if (arg.length < 11) url else args(10)
  val commlab: Array[String] = if (nodesf.isEmpty) Array[String]() else dllCommFile(nodesf)


  override def selectiveProc(v: VertexVisitor, ts: Long, gp: Array[Long]): Unit = {
    val word = v.getPropertyValue("Word").get.asInstanceOf[String]
    if (commlab.contains(word)) {
      var neiLab = v.getOrSetState[Map[Long, Array[Long]]]("neilab", Map[Long, Array[Long]]())
      neiLab=neiLab.updated(ts,gp)
      v.setState("neilab", neiLab)
    }
  }
  override def returnResults(): Any =
    view
      .getVertices()
      .map(vertex =>
        (
          vertex.getPropertyValue("Word").getOrElse(vertex.ID()).toString,
          vertex.getOrSetState[Map[Long, Array[Long]]]("neilab", Map[Long, Array[Long]]()),
          vertex.getState[Array[(Long, Long)]]("mlpalabel")
        )
      )
      .flatMap(f=> f._3.map(x=> (f._1, x._1, x._2, f._2.getOrElse(x._1,Array[Long]()))))
//      .flatMap(f => f._2.map(x => (f._1, x._1, x._2)))
//      //      .flatMap(f =>  f._1.toArray.map(x => (x._2._2, "\""+x._1.toString+f._2+"\"")))
      .groupBy(f => f._1)
      .map(f => (f._1, f._2.map(x=>(x._2,x._3, x._4)).toArray))
//      .map(f => (f._1, f._2.map(x=>(x._2,x._3)).toArray))


  override def processResults(results: ArrayBuffer[Any], timestamp: Long, viewCompleteTime: Long): Unit = {
    val endResults = results.asInstanceOf[ArrayBuffer[immutable.ParHashMap[String, Array[(Long, Long, Array[Long])]]]].flatten
    val selc = "{" + endResults
      .filter(x=> x._2.head._3.nonEmpty)
      .map { wd =>
      val comts =  wd._2.map{cts =>
        s""""${cts._1}": [ ${cts._3.mkString(",")} ]"""
      }.mkString(",")
      s""""${wd._1}": { $comts } """
    }.mkString(",") + "}"

    val comms = s"""[ ${endResults.flatMap(x=> x._2.map(f=>(f._2, x._1+"_"+f._1.toString)))
      .groupBy(f => f._1).mapValues(x => x.map(_._2)).values
      .map(x => s"""["${x.mkString("\",\"")}"]""")
      .mkString(",")}]"""

    val text = s""" {"communities": $comms, "selc": $selc } """
//    println(text)
    writeOut(text, output_file)
  }

//  override def processWindowResults(
//                                     results: ArrayBuffer[Any],
//                                     timestamp: Long,
//                                     windowSize: Long,
//                                     viewCompleteTime: Long
//                                   ): Unit = {
//    val er      = extractData(results)
//    val commtxt = er.communities.map(x => s"""[${x.mkString(",")}]""")
//    val text = s"""{"time":$timestamp,"windowsize":$windowSize,"total":${er.total},"totalIslands":${er.totalIslands},"top5":[${er.top5
//      .mkString(",")}],"""+
//      s""""communities": [${commtxt.mkString(",")}],"""+
//      s""""viewTime":$viewCompleteTime}"""
//    writeOut(text, output_file)
//  }


  //args = [top, weight, maxiter, start, end, layer-size, omega, theta]
//  val theta: Double = if (arg.length < 8) 0.0 else args(7).toDouble
//  val scaled: Boolean = if (arg.length < 8) false else args(7).toBoolean


//  override def weightFunction(v: VertexVisitor, ts: Long): ParMap[Long, Double] = {
//    var nei_weights =
//      (v.getInCEdgesBetween(ts - snapshotSize, ts) ++ v.getOutEdgesBetween(ts - snapshotSize, ts)).map(e =>
//        (e.ID(), e.getPropertyValue(weight).getOrElse(1.0).asInstanceOf[Double])
//      )
//    if (scaled) {
//      val scale = scaling(nei_weights.map(_._2).toArray)
//      nei_weights = nei_weights.map(x => (x._1, x._2 / scale))
//
//    }
//    nei_weights.groupBy(_._1).mapValues(x => x.map(_._2).sum / x.size) // (ID -> Freq)
//  }

//  def scaling(freq: Array[Double]): Double = math.sqrt(freq.map(math.pow(_, 2)).sum)

//  override def processResults(results: ArrayBuffer[Any], timestamp: Long, viewCompleteTime: Long): Unit = {
//    val er = extractData(results)
//    output_file match {
//      case "" =>
//        val commtxt = er.communities.map(x => s"""[${x.mkString(",")}]""")
//        val text = s"""{"time":$timestamp,"top5":[${er.top5
//          .mkString(",")}],"total":${er.total},"totalIslands":${er.totalIslands},""" +
//          s""" "communities": [${commtxt.mkString(",")}],""" +
//          s"""viewTime":$viewCompleteTime}"""
//        println(text)
//      case _ =>
//        val text = s"""{"time":$timestamp,"top5":[${er.top5
//          .mkString(",")}],"total":${er.total},"totalIslands":${er.totalIslands}, "viewTime":$viewCompleteTime}"""
//        println(text)
//        case class Data(comm: Array[String])
//        val writer =
//          ParquetWriter.writer[Data](output_file, ParquetWriter.Options(writeMode = ParquetFileWriter.Mode.OVERWRITE))
//        try er.communities.foreach(c => writer.write(Data(c.toArray)))
//        finally writer.close()
//    }
//  }

  def dllCommFile(url:String): Array[String] ={
    val html = if(url.startsWith("http")) Source.fromURL(url) else Source.fromFile(url)
    html.mkString.split("\n")
  }
}
