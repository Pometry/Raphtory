package com.raphtory

import com.raphtory.api.input.Graph
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.ImmutableProperty
import com.raphtory.api.input.Properties

/**
  * Basic graph builder object
  *
  * Simply reads a file and splits the line by separator param.
  *
  * Should only be used in tests and not anywhere else.
  */
object BasicGraphBuilder extends GraphBuilder[String] {

  def apply(graph: Graph, line: String): Unit =
    if (line.nonEmpty) {
      val fileLine   = line.split(",").map(_.trim)
      val sourceNode = fileLine(0)
      val srcID      = sourceNode.toLong
      val targetNode = fileLine(1)
      val tarID      = targetNode.toLong
      val timeStamp  = fileLine(2).toLong

      graph.addVertex(timeStamp, srcID, Properties(ImmutableProperty("name", sourceNode)))
      graph.addVertex(timeStamp, tarID, Properties(ImmutableProperty("name", targetNode)))
      graph.addEdge(timeStamp, srcID, tarID)

//      logger.debug(s"Finished processing line '$line'.")
    }

}
