package com.raphtory.builders

import com.raphtory.api.input.{Graph, ImmutableProperty, Properties, Type}
import com.raphtory.internals.time.DateTimeParser.{defaultParse => parseDateTime}

object EdgeListBuilder {

  def apply(graph: Graph, line: String, timePosition: Int = 0, srcPosition: Int = 1, dstPosition: Int = 2, datetime: Boolean = false) = {
    if (line.nonEmpty) {
      val fileLine = line.split(",").map(_.trim)
      val timestamp = if (datetime) parseDateTime(fileLine(timePosition)) else fileLine(timePosition).toLong
      val sourceID = fileLine(srcPosition).toLong
      val targetID = fileLine(dstPosition).toLong
      val sourceNode = fileLine(srcPosition)
      val targetNode = fileLine(dstPosition)

      graph.addVertex(timestamp, sourceID, Properties(ImmutableProperty("name", sourceNode)))
      graph.addVertex(timestamp, targetID, Properties(ImmutableProperty("name", targetNode)))
      graph.addEdge(timestamp, sourceID, targetID, Type("source to target relationship"))
    }
  }

}


