package com.raphtory.examples.lotr

import com.raphtory.core.components.Router.{GraphBuilder, RouterWorker}
import com.raphtory.core.model.communication.{EdgeAdd, Type, VertexAdd, _}

import scala.collection.mutable.ListBuffer
import scala.collection.parallel.mutable.ParHashSet
import scala.util.Random

class LOTRGraphBuilder extends GraphBuilder[String]{

  override def parseTuple(tuple: String) = {

    val fileLine = tuple.split(",").map(_.trim)
    val sourceNode = fileLine(0)
    val srcID = assignID(sourceNode)

    val targetNode = fileLine(1)
    val tarID = assignID(targetNode)

    val timeStamp = fileLine(2).toLong

    sendUpdate(VertexAddWithProperties(timeStamp, srcID, Properties(LongProperty("test",Random.nextLong()),ImmutableProperty("name",sourceNode)),Type("Character")))
    sendUpdate(VertexAddWithProperties(timeStamp, tarID, Properties(LongProperty("test2",Random.nextLong()),ImmutableProperty("name",targetNode)),Type("Character")))
    sendUpdate(EdgeAdd(timeStamp,srcID,tarID, Type("Character Co-occurence")))
  }
}
