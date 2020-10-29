package com.raphtory.examples.lotr

import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication.{EdgeAdd, Type, VertexAdd, _}

import scala.collection.mutable.ListBuffer

class LOTRRouter(override val routerId: Int, override val workerID:Int, override val initialManagerCount: Int)
  extends RouterWorker[StringSpoutGoing](routerId,workerID, initialManagerCount) {

  override protected def parseTuple(tuple: StringSpoutGoing): List[GraphUpdate] = {

    val fileLine = tuple.value.split(",").map(_.trim)
    val commands = new ListBuffer[GraphUpdate]()
    val sourceNode = fileLine(0)
    val srcID = assignID(sourceNode)

    val targetNode = fileLine(1)
    val tarID = assignID(targetNode)

    val timeStamp = fileLine(2).toLong

    commands+=(VertexAddWithProperties(timeStamp, srcID, Properties(ImmutableProperty("name",sourceNode)),Type("Character")))
    commands+=(VertexAddWithProperties(timeStamp, tarID, Properties(ImmutableProperty("name",targetNode)),Type("Character")))
    commands+=(EdgeAdd(timeStamp,srcID,tarID, Type("Character Co-occurence")))
    commands.toList
  }
}
