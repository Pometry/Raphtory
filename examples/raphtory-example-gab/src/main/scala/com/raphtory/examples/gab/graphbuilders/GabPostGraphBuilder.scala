package com.raphtory.examples.gab.graphbuilders

import com.raphtory.api.input.GraphBuilder

import java.text.SimpleDateFormat

// The lines sent by the Gab mining spout are read and processed accordingly.
//In this router we needed to transform the data that was sent by the spout by turning it into a epoch value (long value)
// in order to be accepted by Raphtory to create the corresponding entity.
// for the gab dataset, a filter is applied to exclude those lines in where the parent post id ir the parent user id
// is equal to -1. Columns 1 and 4 correspond to the postId and parentPostid in the file.
// Then either the vertex or the edge are created accordingly.

class GabPostGraphBuilder extends GraphBuilder[String] {

  override def parseTuple(tuple: String) = {
    val fileLine   = tuple.split(";").map(_.trim)
    //user wise
//     val sourceNode=fileLine(2).toInt
//     val targetNode=fileLine(5).toInt
    //comment wise
    val sourceNode = fileLine(1).toInt
    val targetNode = fileLine(4).toInt

    if (targetNode > 0) {
      val creationDate = dateToUnixTime(timestamp = fileLine(0).slice(0, 19))
      addVertex(creationDate, sourceNode)
      addVertex(creationDate, targetNode)
      addEdge(creationDate, sourceNode, targetNode)
    }
  }

  def dateToUnixTime(timestamp: => String): Long = {
    val sdf   = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    val dt    = sdf.parse(timestamp)
    val epoch = dt.getTime
    epoch
  }
}
