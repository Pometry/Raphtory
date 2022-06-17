package com.raphtory.lotrtest

import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.ImmutableProperty
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Type

class LOTRGraphBuilder() extends GraphBuilder[String] {

  override def parseTuple(tuple: String): Unit = {
    println(s"TUPLE TIME! ${tuple}")
    val fileLine   = tuple.replace("\"", "").split(",").map(_.trim)
    val sourceNode = fileLine(0)
    val srcID      = assignID(sourceNode)
    val targetNode = fileLine(1)
    val tarID      = assignID(targetNode)
    val timeStamp  = fileLine(2).toLong

    addVertex(
            timeStamp,
            srcID,
            Properties(ImmutableProperty("name", sourceNode)),
            Type("Character")
    )
    addVertex(
            timeStamp,
            tarID,
            Properties(ImmutableProperty("name", targetNode)),
            Type("Character")
    )
    addEdge(timeStamp, srcID, tarID, Type("Character Co-occurence"))

    println("DONE!")
  }
}
