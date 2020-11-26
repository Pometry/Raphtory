package com.raphtory.examples.test.actors

import com.raphtory.core.components.Router.{GraphBuilder, RouterWorker}
import com.raphtory.core.components.Spout.Spout
import com.raphtory.core.model.communication.{EdgeAdd, GraphUpdate, VertexAddWithProperties}

import scala.collection.mutable.ListBuffer
import scala.collection.parallel.mutable.ParHashSet

class TriangleGraphBuilder extends GraphBuilder[String] {
  override def parseTuple(tuple: String)= {
    val command  = tuple.split(",")
    val msgTime = command(0).toLong
    val sourceID = command(1).toLong
    val destID = command(2).toLong
    sendUpdate(EdgeAdd(msgTime, sourceID, destID))
  }
}
