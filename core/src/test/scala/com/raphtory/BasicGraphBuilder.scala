package com.raphtory

import com.raphtory.api.input.Graph
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.ImmutableProperty
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Properties._

/**
  * Basic graph builder object
  *
  * Simply reads a file and splits the line by separator param.
  *
  * Should only be used in tests and not anywhere else.
  */
class BasicGraphBuilder extends GraphBuilder[String] {

  override def parse(graph: Graph, line: String): Unit =
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

object BasicGraphBuilder {
  def apply() = new BasicGraphBuilder()
}
