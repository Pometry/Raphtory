package com.raphtory.algorithms

import java.time.LocalDateTime

import com.github.mjakubowski84.parquet4s.{ParquetReader, ParquetWriter}
import com.raphtory.core.model.analysis.entityVisitors.VertexVisitor
import org.apache.parquet.hadoop.ParquetFileWriter

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ParMap
import scala.reflect.io.Path

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
  val word: Array[String] =  if (arg.length < 8) Array("advert") else args(7).split("-")
  override def setup(): Unit = {
    val t1 = System.currentTimeMillis()
    view.getVertices().foreach { vertex =>
      // Assign random labels for all instances in time of a vertex as Map(ts, lab)
      val tlabels = mutable.TreeMap[Long, (Long, Long)]()  //im: send tuples instead of TreeMap and build the TM for processing only
      snapshots
        .filter(t => vertex.aliveAtWithWindow(t, snapshotSize))
        .foreach(tlabels.put(_, (scala.util.Random.nextLong(), scala.util.Random.nextLong())))
      vertex.setState("mlpalabel", tlabels)
      vertex.messageAllNeighbours((vertex.ID(), tlabels))
    }
    println(" Setup timing - wID: %s    Time: %s    ExecTime: %s".format(workerID, LocalDateTime.now(), System.currentTimeMillis() - t1))
  }

  override def doSomething(v: VertexVisitor, gp: Array[Long]): Unit = {
    val wd = v.getPropertyValue("Word").get
    if (word.contains(wd)) {
      val lab = v.getState[mutable.TreeMap[Long, (Long, Long)]]("mlpalabel").head
      println("Superstep: %s   Time: %s     Vertex: %s    ID: %s   Label: %s"
        .format(view.superStep(),LocalDateTime.now(), wd, v.ID(),  lab._2._2))
    }
  }

  override def processResults(results: ArrayBuffer[Any], timestamp: Long, viewCompleteTime: Long): Unit = {
    val er      = extractData(results)
    val text = s"""{"time":$timestamp,"top5":[${er.top5
      .mkString(",")}],"total":${er.total},"totalIslands":${er.totalIslands}, "viewTime":$viewCompleteTime}"""

    output_file match {
      case "" => println(text)
      case _  =>
        println(text)
        case class Data(comm: Array[String])
        val writer = ParquetWriter.writer[Data](output_file,ParquetWriter.Options(
          writeMode = ParquetFileWriter.Mode.OVERWRITE))
        try er.communities.foreach(c=> writer.write(Data(c.toArray)))
        finally writer.close()
    }
  }
}
//
//  override def weightFunction(v: VertexVisitor, ts: Long): ParMap[Long, Long] =
//    (v.getInCEdgesBetween(ts - snapshotSize, ts) ++ v.getOutEdgesBetween(ts - snapshotSize, ts))
//      .filter(e => e.getPropertyValueAt("ScaledFreq", ts).getOrElse(1.0).asInstanceOf[Double] > theta)
//      .map(e => (e.ID(), e.getPropertyValue(weight).getOrElse(1L).asInstanceOf[Long])) //  im: fix this one after pulling new changes
//      .groupBy(_._1)
//      .mapValues(x => x.map(_._2).sum / x.size)
//}
