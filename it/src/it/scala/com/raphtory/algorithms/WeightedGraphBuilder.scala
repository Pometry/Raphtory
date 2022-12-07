package com.raphtory.algorithms

import com.raphtory.api.input.Graph
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.ImmutableString
import com.raphtory.api.input.MutableLong
import com.raphtory.api.input.Properties

object WeightedGraphBuilder extends GraphBuilder[String] {

  def apply(graph: Graph, tuple: String): Unit = {
    val line       = tuple.split(",")
    val sourceNode = line(0)
    val srcID      = sourceNode.toLong
    val targetNode = line(1)
    val tarID      = targetNode.toLong
    val timeStamp  = line(2).toLong
    val weight     = line(3).toLong

    graph.addVertex(timeStamp, srcID, Properties(ImmutableString("name", sourceNode)))
    graph.addVertex(timeStamp, tarID, Properties(ImmutableString("name", targetNode)))
    graph.addEdge(timeStamp, srcID, tarID, Properties(MutableLong("weight", weight)))

//    logger.debug(s"Finished processing line '$line'.")
  }

}
