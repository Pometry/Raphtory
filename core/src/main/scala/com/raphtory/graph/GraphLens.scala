package com.raphtory.graph

import com.raphtory.graph.visitor.Vertex
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

/** !DoNotDocument */
abstract class GraphLens(jobId: String, start: Long, end: Long) {

  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  //TODO REMOVE THESE STUBS ONCE ANALYSERS CLEANED
  def getVertices(): List[Vertex] =
    List()

  def getMessagedVertices(): List[Vertex] =
    List()
}
