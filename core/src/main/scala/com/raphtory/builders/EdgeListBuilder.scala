package com.raphtory.builders

import com.raphtory.api.input.{Graph, ImmutableProperty, Properties, Type}
import com.raphtory.internals.time.DateTimeParser.{defaultParse => parseDateTime}

object EdgeListBuilder {

  def apply(graph: Graph, line: String, timePosition: Int = 0, sourcePosition: Int = 1, targetPosition: Int = 2, datetime: Boolean = false) = {
    if (line.nonEmpty) {
      val fileLine = line.split(",").map(_.trim)
      val timestamp = if (datetime) parseDateTime(fileLine(timePosition)) else fileLine(timePosition).toLong
      val sourceID = fileLine(sourcePosition).toLong
      val targetID = fileLine(targetPosition).toLong
      val sourceNode = fileLine(sourcePosition)
      val targetNode = fileLine(targetPosition)

      graph.addVertex(timestamp, sourceID, Properties(ImmutableProperty("name", sourceNode)))
      graph.addVertex(timestamp, targetID, Properties(ImmutableProperty("name", targetNode)))
      graph.addEdge(timestamp, sourceID, targetID, Type("source to target relationship"))
    }
  }

}


