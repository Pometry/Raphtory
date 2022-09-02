package com.raphtory.algorithms

import com.raphtory.api.input.ImmutableProperty
import com.raphtory.api.input.LongProperty
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Properties._
import com.raphtory.internals.graph.Graph
import com.raphtory.internals.graph.GraphBuilder

class WeightedGraphBuilder extends GraphBuilder[String] {

  override def parse(graph: Graph, tuple: String): Unit = {
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

object WeightedGraphBuilder {
  def apply() = new WeightedGraphBuilder
}
