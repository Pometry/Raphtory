package com.raphtory.examples.test.actors

import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.components.Spout.SpoutTrait
import com.raphtory.core.model.communication.{EdgeAdd, GraphUpdate, StringSpoutGoing, VertexAddWithProperties}

import scala.collection.mutable.ListBuffer
import scala.collection.parallel.mutable.ParHashSet

class TriangleTestRouter (override val routerId: Int,override val workerID:Int, override val initialManagerCount: Int, override val initialRouterCount: Int)
  extends RouterWorker[StringSpoutGoing](routerId,workerID, initialManagerCount, initialRouterCount) {
  override protected def parseTuple(tuple: StringSpoutGoing): ParHashSet[GraphUpdate] = {
    val command  = tuple.value.split(",")
    val msgTime = command(0).toLong
    val sourceID = command(1).toLong
    val destID = command(2).toLong
    ParHashSet((EdgeAdd(msgTime, sourceID, destID)))
  }
}
