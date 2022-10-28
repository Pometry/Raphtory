package com.raphtory.internals.storage.pojograph.messaging

import com.raphtory.api.analysis.visitor.Vertex
import com.raphtory.internals.graph.VertexUpdate.ordering
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

object MessageAggregation {
  private val logger: Logger                   =
    Logger(LoggerFactory.getLogger(this.getClass))

  def noAggregation(Vertex:Vertex): Unit =
    logger.info(s"Hello there ${Vertex.ID}")
//    None

  def idLoggerTest(Vertex:Vertex): Unit =
    logger.info(s"Konichiwa ${Vertex.ID}")

  def max(Vertex:Vertex): Unit =
    Vertex.messageQueue.max

  def min(Vertex:Vertex): Unit =
    Vertex.messageQueue.min
}
