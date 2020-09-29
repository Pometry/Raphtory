package com.raphtory.examples.tsvnet

import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication.{EdgeAdd, Type, VertexAdd}

/** Spout for network datasets of the form SRC_NODE_ID DEST_NODE_ID TIMESTAMP */
class TSVRouter(override val routerId: Int, override val workerID:Int, override val initialManagerCount: Int) extends RouterWorker {

  def parseTuple(record: Any): Unit = {
    val fileLine = record.asInstanceOf[String].split(" ").map(_.trim)
    //user wise
    val sourceNode = fileLine(0).toInt
    val targetNode = fileLine(1).toInt
    val creationDate = fileLine(2).toLong



    //comment wise
    // val sourceNode=fileLine(1).toInt
    //val targetNode=fileLine(4).toInt
    if (targetNode > 0) {
      val creationDate = fileLine(2).toLong
      sendGraphUpdate(VertexAdd(creationDate, sourceNode, Type("User")))
      sendGraphUpdate(VertexAdd(creationDate, targetNode, Type("User")))
      sendGraphUpdate(EdgeAdd(creationDate, sourceNode, targetNode, Type("User to User")))

    }

  }
}
