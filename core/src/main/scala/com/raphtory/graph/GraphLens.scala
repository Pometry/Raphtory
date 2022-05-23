package com.raphtory.graph

import com.raphtory.graph.visitor.Vertex
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

/** @note DoNotDocument */
abstract class GraphLens(jobId: String, start: Long, end: Long) {

  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  //TODO REMOVE THESE STUBS ONCE ANALYSERS CLEANED
  def getVertices(): List[Vertex] =
    List()

  def getMessagedVertices(): List[Vertex] =
    List()
}
