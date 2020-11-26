package com.raphtory.examples.lotr

import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication.{EdgeAdd, Type, VertexAdd, _}

import scala.collection.mutable.ListBuffer
import scala.collection.parallel.mutable.ParHashSet
import scala.util.Random

class LOTRRouter(override val routerId: Int, override val workerID:Int, override val initialManagerCount: Int, override val initialRouterCount: Int)
  extends RouterWorker[String](routerId,workerID, initialManagerCount, initialRouterCount) {

  override protected def parseTuple(tuple: String): ParHashSet[GraphUpdate] = {

    val fileLine = tuple.split(",").map(_.trim)
    val commands = new ParHashSet[GraphUpdate]()
    val sourceNode = fileLine(0)
    val srcID = assignID(sourceNode)

    val targetNode = fileLine(1)
    val tarID = assignID(targetNode)

    val timeStamp = fileLine(2).toLong

    commands+=(VertexAddWithProperties(timeStamp, srcID, Properties(LongProperty("test",Random.nextLong()),ImmutableProperty("name",sourceNode)),Type("Character")))
    commands+=(VertexAddWithProperties(timeStamp, tarID, Properties(LongProperty("test2",Random.nextLong()),ImmutableProperty("name",targetNode)),Type("Character")))
    commands+=(EdgeAdd(timeStamp,srcID,tarID, Type("Character Co-occurence")))
    commands
  }
}
