package com.raphtory.lotrtest

import com.raphtory.api.input.ImmutableProperty
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Type
import com.raphtory.internals.graph.Graph
import com.raphtory.internals.graph.GraphBuilder

class LOTRGraphBuilder() extends GraphBuilder[String] {

  override def parse(graph: Graph, tuple: String): Unit = {
    val fileLine   = tuple.replace("\"", "").split(",").map(_.trim)
    val sourceNode = fileLine(0)
    val srcID      = assignID(sourceNode)
    val targetNode = fileLine(1)
    val tarID      = assignID(targetNode)
    val timeStamp  = fileLine(2).toLong

    graph.addVertex(
            timeStamp,
            srcID,
            Properties(ImmutableProperty("name", sourceNode)),
            Type("Character")
    )
    graph.addVertex(
            timeStamp,
            tarID,
            Properties(ImmutableProperty("name", targetNode)),
            Type("Character")
    )
    graph.addEdge(timeStamp, srcID, tarID, Type("Character Co-occurence"))
  }
}
