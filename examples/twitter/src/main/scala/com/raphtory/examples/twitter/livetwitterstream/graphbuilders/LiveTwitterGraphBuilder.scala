package com.raphtory.examples.twitter.livetwitterstream.graphbuilders

import com.raphtory.api.input.ImmutableProperty
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Type
import com.raphtory.internals.graph.Graph
import com.raphtory.internals.graph.GraphBuilder

class LiveTwitterGraphBuilder extends GraphBuilder[String] {

  override def parse(graph: Graph, tuple: String): Unit = {
    val fileLine   = tuple.split(" ").map(_.trim)
    val sourceNode = fileLine(0)
    val srcID      = sourceNode.toLong
    val targetNode = fileLine(1)
    val tarID      = targetNode.toLong
    val timeStamp  = fileLine(2).toLong

    graph.addVertex(timeStamp, srcID, Properties(ImmutableProperty("name", sourceNode)), Type("User"))
    graph.addVertex(timeStamp, tarID, Properties(ImmutableProperty("name", targetNode)), Type("User"))
    graph.addEdge(timeStamp, srcID, tarID, Type("Follow"))
  }
}
