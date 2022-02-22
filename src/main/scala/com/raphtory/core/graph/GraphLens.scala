package com.raphtory.core.graph

import com.raphtory.core.graph.visitor.Vertex
import com.raphtory.core.time.Interval
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

/** @DoNotDocument */
abstract class GraphLens(jobId: String, timestamp: Long, window: Option[Interval]) {

  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  //TODO REMOVE THESE STUBS ONCE ANALYSERS CLEANED
  def getVertices(): List[Vertex] =
    List()

  def getMessagedVertices(): List[Vertex] =
    List()
}
