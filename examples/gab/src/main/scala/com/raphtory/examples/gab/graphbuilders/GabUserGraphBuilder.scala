package com.raphtory.examples.gab.graphbuilders

import com.raphtory.api.input.Graph
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Type

import java.text.SimpleDateFormat

// The lines sent by the Gab mining spout are read and processed accordingly.
//In this router we needed to transform the data that was sent by the spout by turning it into a epoch value (long value)
// in order to be accepted by Raphtory to create the corresponding entity.
// for the gab dataset, a filter is applied to exclude those lines in where the parent post id ir the parent user id
// is equal to -1. Columns 2 and 5 correspond to the userid and parentUserid in the file.
// Then either the vertex or the edge are created accordingly.

object GabUserGraphBuilder extends GraphBuilder[String] {

  def apply(graph: Graph, tuple: String) = {
    val fileLine   = tuple.split(";").map(_.trim)
    //user wise
    val sourceNode = fileLine(2).toInt
    val targetNode = fileLine(5).toInt
    //comment wise
    // val sourceNode=fileLine(1).toInt
    //val targetNode=fileLine(4).toInt
    if (targetNode > 0 && targetNode != sourceNode) {
      val creationDate = dateToUnixTime(timestamp = fileLine(0).slice(0, 19))
      graph.addVertex(creationDate, sourceNode, Type("User"))
      graph.addVertex(creationDate, targetNode, Type("User"))
      graph.addEdge(creationDate, sourceNode, targetNode, Type("User to User"))
//      sendGraphUpdate(VertexAddWithProperties(creationDate, sourceNode, Properties(StringProperty("test1","value1"),StringProperty("test2","Value2")),Type("User")))
//      sendGraphUpdate(VertexAddWithProperties(creationDate, targetNode, Properties(StringProperty("test1","value1"),StringProperty("test2","Value2")),Type("User")))
//      sendGraphUpdate(EdgeAddWithProperties(creationDate, sourceNode, targetNode, Properties(StringProperty("test1","value1"),StringProperty("test2","Value2")),Type("User To User")))
    }
  }

  def dateToUnixTime(timestamp: => String): Long = {
    //if(timestamp == null) return null;
    // println("TIME FUNC: "+ timestamp)
    //val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'+'HH:mm")
    val sdf   = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    val dt    = sdf.parse(timestamp)
    val epoch = dt.getTime
    //println("*******EPOCH: "+epoch)
    epoch
    //=======
    //    val epoch = dt.getTime
    //   // println(epoch)
    //    epoch
    //>>>>>>> upstream/master
  }
}
