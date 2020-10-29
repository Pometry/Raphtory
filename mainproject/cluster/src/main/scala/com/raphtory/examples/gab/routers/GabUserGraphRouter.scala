package com.raphtory.examples.gab.routers

import java.text.SimpleDateFormat

import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication.Type
import com.raphtory.core.model.communication._

import scala.collection.mutable.ListBuffer
import scala.util.Random

// The lines sent by the Gab mining spout are read and processed accordingly.
//In this router we needed to transform the data that was sent by the spout by turning it into a epoch value (long value)
// in order to be accepted by Raphtory to create the corresponding entity.
// for the gab dataset, a filter is applied to exclude those lines in where the parent post id ir the parent user id
// is equal to -1. Columns 2 and 5 correspond to the userid and parentUserid in the file.
// Then either the vertex or the edge are created accordingly.

class GabUserGraphRouter(override val routerId: Int,override val workerID:Int, override val initialManagerCount: Int)
  extends RouterWorker[StringSpoutGoing](routerId,workerID, initialManagerCount) {

  override protected def parseTuple(tuple: StringSpoutGoing): List[GraphUpdate] = {
    val fileLine = tuple.value.split(";").map(_.trim)
    //user wise
    val sourceNode = fileLine(2).toInt
    val targetNode = fileLine(5).toInt
    val commands = new ListBuffer[GraphUpdate]()
    //comment wise
    // val sourceNode=fileLine(1).toInt
    //val targetNode=fileLine(4).toInt
    if (targetNode > 0 && targetNode != sourceNode) {
      val creationDate = dateToUnixTime(timestamp = fileLine(0).slice(0, 19))
      commands+=(VertexAdd(creationDate, sourceNode, Type("User")))
      commands+=(VertexAdd(creationDate, targetNode, Type("User")))
      commands+=(EdgeAdd(creationDate, sourceNode, targetNode, Type("User to User")))
//      sendGraphUpdate(VertexAddWithProperties(creationDate, sourceNode, Properties(StringProperty("test1","value1"),StringProperty("test2","Value2")),Type("User")))
//      sendGraphUpdate(VertexAddWithProperties(creationDate, targetNode, Properties(StringProperty("test1","value1"),StringProperty("test2","Value2")),Type("User")))
//      sendGraphUpdate(EdgeAddWithProperties(creationDate, sourceNode, targetNode, Properties(StringProperty("test1","value1"),StringProperty("test2","Value2")),Type("User To User")))
    }
  commands.toList
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
