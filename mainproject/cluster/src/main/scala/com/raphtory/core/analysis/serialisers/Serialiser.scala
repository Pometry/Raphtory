package com.raphtory.core.analysis.serialisers

import java.io.{BufferedWriter, File, FileWriter}

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.API.entityVisitors.{EdgeVisitor, VertexVisitor}

import scala.collection.mutable.ArrayBuffer

abstract class Serialiser extends Analyser(null){

  val path  = s"${sys.env.getOrElse("SERIALISER_PATH", "")}"


  def serialiseVertex(v:VertexVisitor):String
  def serialiseEdge(e:EdgeVisitor):String
  def startOfFile():String
  def middleOfFile():String
  def endOfFile():String

  def fileExtension(): String = {"txt"}
  def rowDelimeter(): String = {",\n"}

  override def returnResults(): Any = {
    val serialisedEntities = view.getVertices().map { vertex =>
      (serialiseVertex(vertex),vertex.getOutEdges.map(e=> serialiseEdge(e)).toArray)
    }
    (serialisedEntities.map(x=>x._1).toArray,serialisedEntities.flatMap(x=>x._2).toArray)
  }

  override def defineMaxSteps(): Int = 1

  override def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = {
    val serialisedResults = results.asInstanceOf[ArrayBuffer[(Array[String],Array[String])]]
    val file = new File(s"$path/Raphtory_Snapshot_$timeStamp.${fileExtension()}")
    write((serialisedResults.flatMap(x=>x._1).toArray,serialisedResults.flatMap(x=>x._2).toArray),file)
  }

  override def processWindowResults(results: ArrayBuffer[Any], timestamp: Long, windowSize: Long, viewCompleteTime: Long): Unit = {
    val serialisedResults = results.asInstanceOf[ArrayBuffer[(Array[String],Array[String])]]
    val file = new File(s"$path/Raphtory_Snapshot_${timestamp}_$windowSize.${fileExtension()}")
    write((serialisedResults.flatMap(x=>x._1).toArray,serialisedResults.flatMap(x=>x._2).toArray),file)
  }

  def write(serialisedResults:(Array[String],Array[String]),file:File) = {
//    println("write :"+serialisedResults._1.length+":"++serialisedResults._2.length)
    println("vertices & edges", serialisedResults._1.length, serialisedResults._2.length)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(startOfFile())
    bw.write(serialisedResults._1.mkString(rowDelimeter()))
    bw.write(middleOfFile())
    bw.write(serialisedResults._2.mkString(rowDelimeter()))
    bw.write(endOfFile())
    bw.newLine()
    bw.close()
  }

  override def analyse(): Unit = {}
  override def setup(): Unit = {}
}
