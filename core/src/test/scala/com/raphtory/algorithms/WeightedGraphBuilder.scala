package com.raphtory.algorithms

import com.raphtory.api.input.Graph
import com.raphtory.api.input.ImmutableProperty
import com.raphtory.api.input.LongProperty
import com.raphtory.api.input.Properties
import com.raphtory.internals.graph.GraphBuilder

object WeightedGraphBuilder {

  def parse(graph: Graph, tuple: String): Unit = {
    val line       = tuple.split(",")
    val sourceNode = line(0)
    val srcID      = sourceNode.toLong
    val targetNode = line(1)
    val tarID      = targetNode.toLong
    val timeStamp  = line(2).toLong
    val weight     = line(3).toLong

    graph.addVertex(timeStamp, srcID, Properties(ImmutableProperty("name", sourceNode)))
    graph.addVertex(timeStamp, tarID, Properties(ImmutableProperty("name", targetNode)))
    graph.addEdge(timeStamp, srcID, tarID, Properties(LongProperty("weight", weight)))

//    logger.debug(s"Finished processing line '$line'.")
  }

}
