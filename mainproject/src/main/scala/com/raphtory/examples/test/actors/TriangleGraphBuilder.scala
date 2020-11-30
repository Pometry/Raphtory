package com.raphtory.examples.test.actors

import com.raphtory.core.actors.Router.GraphBuilder
import com.raphtory.core.model.communication.EdgeAdd

class TriangleGraphBuilder extends GraphBuilder[String] {
  override def parseTuple(tuple: String)= {
    val command  = tuple.split(",")
    val msgTime = command(0).toLong
    val sourceID = command(1).toLong
    val destID = command(2).toLong
    sendUpdate(EdgeAdd(msgTime, sourceID, destID))
  }
}
