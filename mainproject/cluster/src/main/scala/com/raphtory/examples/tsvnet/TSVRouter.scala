package com.raphtory.examples.tsvnet

import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication.{EdgeAdd, GraphUpdate, StringSpoutGoing, Type, VertexAdd}

import scala.collection.mutable.ListBuffer

/** Spout for network datasets of the form SRC_NODE_ID DEST_NODE_ID TIMESTAMP */
class TSVRouter(override val routerId: Int, override val workerID:Int, override val initialManagerCount: Int, override val initialRouterCount: Int)
  extends RouterWorker[StringSpoutGoing](routerId,workerID, initialManagerCount, initialRouterCount) {
  val commands = new ListBuffer[GraphUpdate]()
  override protected def parseTuple(tuple: StringSpoutGoing): List[GraphUpdate] = {
    val fileLine = tuple.value.split(" ").map(_.trim)
    //user wise
    val sourceNode = fileLine(0).toInt
    val targetNode = fileLine(1).toInt
    val creationDate = fileLine(2).toLong



    //comment wise
    // val sourceNode=fileLine(1).toInt
    //val targetNode=fileLine(4).toInt
    if (targetNode > 0) {
      val creationDate = fileLine(2).toLong
      commands+=(VertexAdd(creationDate, sourceNode, Type("User")))
      commands+=(VertexAdd(creationDate, targetNode, Type("User")))
      commands+=(EdgeAdd(creationDate, sourceNode, targetNode, Type("User to User")))

    }
    commands.toList
  }
}
