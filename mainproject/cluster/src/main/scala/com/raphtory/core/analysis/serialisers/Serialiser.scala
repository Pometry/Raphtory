package com.raphtory.core.analysis.serialisers

import java.io.{BufferedWriter, File, FileWriter}

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.API.entityVisitors.{EdgeVisitor, VertexVisitor}

import scala.collection.mutable.ArrayBuffer

abstract class Serialiser extends Analyser(null){

  def serialiseVertex(v:VertexVisitor):String
  def serialiseEdge(e:EdgeVisitor):String

  override def returnResults(): Any = {
    val serialisedEntities = view.getVertices().map { vertex =>
      (serialiseVertex(vertex),vertex.getOutEdges.map(e=> serialiseEdge(e)).toArray)
    }
    (serialisedEntities.map(x=>x._1).toArray,serialisedEntities.flatMap(x=>x._2))
  }

  override def defineMaxSteps(): Int = 1

  override def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = {
    val serialisedResults = results.asInstanceOf[(Array[String],Array[String])]
    val file = new File(s"Raphtory_Snapshot_$timeStamp.txt")
    write(serialisedResults,file)
  }

  override def processWindowResults(results: ArrayBuffer[Any], timestamp: Long, windowSize: Long, viewCompleteTime: Long): Unit = {
    val serialisedResults = results.asInstanceOf[(Array[String],Array[String])]
    val file = new File(s"Raphtory_Snapshot_${timestamp}_$windowSize.txt")
    write(serialisedResults,file)

  }

  def write(serialisedResults:(Array[String],Array[String]),file:File) = {
    val bw = new BufferedWriter(new FileWriter(file))
    for (line <- serialisedResults._1) {
      bw.write(line)
    }
    for (line <- serialisedResults._2) {
      bw.write(line)
    }
    bw.close()
  }

  override def analyse(): Unit = {}
  override def setup(): Unit = {}
}
