package com.raphtory.examples.gab.graphbuilders

import java.text.SimpleDateFormat

import com.raphtory.core.actors.Router.GraphBuilder
import com.raphtory.core.model.communication.{Type, _}

// The lines sent by the Gab mining spout are read and processed accordingly.
//In this router we needed to transform the data that was sent by the spout by turning it into a epoch value (long value)
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
      sendUpdate(VertexAdd(creationDate, sourceNode, Type("User")))
      sendUpdate(VertexAdd(creationDate, targetNode, Type("User")))
      sendUpdate(EdgeAdd(creationDate, sourceNode, targetNode, Type("User to User")))
//      sendGraphUpdate(VertexAddWithProperties(creationDate, sourceNode, Properties(StringProperty("test1","value1"),StringProperty("test2","Value2")),Type("User")))
//      sendGraphUpdate(VertexAddWithProperties(creationDate, targetNode, Properties(StringProperty("test1","value1"),StringProperty("test2","Value2")),Type("User")))
//      sendGraphUpdate(EdgeAddWithProperties(creationDate, sourceNode, targetNode, Properties(StringProperty("test1","value1"),StringProperty("test2","Value2")),Type("User To User")))
    }
  }

  def dateToUnixTime(timestamp: => String): Long = {
    //if(timestamp == null) return null;
    // println("TIME FUNC: "+ timestamp)
    //val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'+'HH:mm")
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    //println(sdf)
    val dt = sdf.parse(timestamp)
    //println(dt)
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
