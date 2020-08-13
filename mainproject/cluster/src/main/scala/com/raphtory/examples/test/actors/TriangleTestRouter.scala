package com.raphtory.examples.test.actors

import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.components.Spout.SpoutTrait
import com.raphtory.core.model.communication.{EdgeAdd, VertexAddWithProperties}

class TriangleTestRouter (override val routerId: Int,override val workerID:Int, override val initialManagerCount: Int) extends RouterWorker {
  override protected def parseTuple(tuple: Any): Unit = {
    val command  = tuple.asInstanceOf[String].split(",")
    val msgTime = command(0).toLong
    val sourceID = command(1).toLong
    val destID = command(2).toLong
    sendGraphUpdate(EdgeAdd(msgTime, sourceID, destID))
  }
}
