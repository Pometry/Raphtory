package com.raphtory.dev.gab.graphbuilders

import java.text.SimpleDateFormat
import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.implementations.generic.messaging._
import com.raphtory.core.model.graph.Type

// The lines sent by the Gab mining spout are read and processed accordingly.
//In this builder we needed to transform the data that was sent by the spout by turning it into a epoch value (long value)
// in order to be accepted by Raphtory to create the corresponding entity.
// for the gab dataset, a filter is applied to exclude those lines in where the parent post id ir the parent user id
// is equal to -1. Columns 2 and 5 correspond to the userid and parentUserid in the file.
// Then either the vertex or the edge are created accordingly.

class GabUserGraphBuilder extends GraphBuilder[String] {

  override def parseTuple(tuple: String) = {
    val fileLine = tuple.split(";").map(_.trim)
    //user wise
    val sourceNode = fileLine(2).toInt
    val targetNode = fileLine(5).toInt
    //comment wise
    // val sourceNode=fileLine(1).toInt
    //val targetNode=fileLine(4).toInt
    if (targetNode > 0 && targetNode != sourceNode) {
      val creationDate = dateToUnixTime(timestamp = fileLine(0).slice(0, 19))
      //addVertex(creationDate, sourceNode, Type("User"))
      //addVertex(creationDate, targetNode, Type("User"))
      addEdge(creationDate, sourceNode, targetNode)
//      sendGraphUpdate(VertexAddWithProperties(creationDate, sourceNode, Properties(StringProperty("test1","value1"),StringProperty("test2","Value2")),Type("User")))
//      sendGraphUpdate(VertexAddWithProperties(creationDate, targetNode, Properties(StringProperty("test1","value1"),StringProperty("test2","Value2")),Type("User")))
//      sendGraphUpdate(EdgeAddWithProperties(creationDate, sourceNode, targetNode, Properties(StringProperty("test1","value1"),StringProperty("test2","Value2")),Type("User To User")))
    }
  }

  def dateToUnixTime(timestamp: => String): Long = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    val dt = sdf.parse(timestamp)
    dt.getTime
  }
}
