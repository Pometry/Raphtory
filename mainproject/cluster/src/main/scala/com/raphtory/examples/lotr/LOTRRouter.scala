package com.raphtory.examples.lotr

import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication.{EdgeAdd, Type, VertexAdd, _}

class LOTRRouter(override val routerId: Int, override val workerID:Int, override val initialManagerCount: Int) extends RouterWorker {

  def parseTuple(record: Any): Unit = {

    val fileLine = record.asInstanceOf[String].split(",").map(_.trim)

    val sourceNode = fileLine(0)
    val srcID = assignID(sourceNode)

    val targetNode = fileLine(1)
    val tarID = assignID(targetNode)

    val timeStamp = fileLine(2).toLong

    sendGraphUpdate(VertexAddWithProperties(timeStamp, srcID, Properties(ImmutableProperty("name",sourceNode)),Type("Character")))
    sendGraphUpdate(VertexAddWithProperties(timeStamp, tarID, Properties(ImmutableProperty("name",targetNode)),Type("Character")))
    sendGraphUpdate(EdgeAdd(timeStamp,srcID,tarID, Type("Character Co-occurence")))
  }
}
