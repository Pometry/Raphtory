//package com.raphtory.dev.algorithms
//
//import com.github.mjakubowski84.parquet4s.ParquetWriter
//import com.raphtory.core.analysis.entity.Vertex
//import org.apache.parquet.hadoop.ParquetFileWriter
//
//import scala.collection.mutable.ArrayBuffer
//import scala.collection.parallel.{ParMap, immutable}
//import scala.io.Source
//import scala.reflect.io.Path
//
///**
//A version of MultiLayerLPA that gives freedom over filering the edges to consider
//-- Work specific to the Word Semantics Project
// **/
//object MultiLayerLPAparams {
//  def apply(args: Array[String]): MultiLayerLPAparams = new MultiLayerLPAparams(args)
//}
//class MultiLayerLPAparams(args: Array[String]) extends MultiLayerLPA(args) {
//  val url="https://raphtorydatasets.blob.core.windows.net/top-tier/misc/selnodes.csv"
//  val nodesf: String = if (arg.length < 11) url else args(10)
//  val commlab: Array[String] = if (nodesf.isEmpty) Array[String]() else dllCommFile(nodesf)
//
//  def dllCommFile(url:String): Array[String] ={
//    val html = if(url.startsWith("http")) Source.fromURL(url) else Source.fromFile(url)
//    html.mkString.split("\n")
//  }
//  override def selectiveProc(v: Vertex, ts: Long, gp: Array[Long]): Unit = {
//    val word = v.getPropertyValue("Word").get.asInstanceOf[String]
//    if (commlab.contains(word)) {
//      var neiLab = v.getOrSetState[Map[Long, Array[Long]]]("neilab",Map[Long, Array[Long]]())
//      neiLab=neiLab.updated(ts,gp)
//      v.setState("neilab", neiLab)
//    }
//  }
//  override def returnResults(): Any =
//    view
//      .getVertices()
//      .map(vertex =>
//        (
//          vertex.getPropertyValue("Word").getOrElse(vertex.ID()).toString,
//          vertex.getOrSetState[Map[Long, Array[Long]]]("neilab", Map[Long, Array[Long]]()),
//          vertex.getState[Array[(Long, Long)]]("mlpalabel")
//        )
//      )
//      .flatMap(f => f._2.map(x => (f._1, x._1, x._2)))
////          //  .flatMap(f =>  f._1.toArray.map(x => (x._2._2, "\""+x._1.toString+f._2+"\"")))
//      .groupBy(f => f._1)
//      .map(f => (f._1, f._2.map(x=>(x._2,x._3, x._4)).toArray))
////      .map(f => (f._1, f._2.map(x=>(x._2,x._3)).toArray))
//
//
//  override def extractResults(results: List[Any]): Map[String,Any]  = {
////    val er = extractData(results)
////  override def processResults(results: ArrayBuffer[Any], timestamp: Long, viewCompleteTime: Long): Unit = {
//    val endResults = results.asInstanceOf[List[immutable.ParHashMap[String, Array[(Long, Array[Long])]]]].flatten
//    val text = "{" + endResults.map { wd =>
//      val comts =  wd._2.map{cts=>
//        s""""${cts._1}": [ ${cts._2.mkString(",")} ]"""
//      }.mkString(",")
//      s""""${wd._1}": { $comts } """
//    }.mkString(",") + "}"
//    output_file match {
//      case "" => println(text)
//      //        case "mongo" => mongo.getDB(dbname).getCollection(jobID).insert(JSON.parse(data).asInstanceOf[DBObject])
//      case _ => Path(output_file).createFile().appendAll(text + "\n")
//    }
//    Map[String,Any]()
//  }
//
////  def dllCommFile(url:String): Array[String] ={
////    val html = if(url.startsWith("http")) Source.fromURL(url) else Source.fromFile(url)
////    html.mkString.split("\n")
////  }
//}
