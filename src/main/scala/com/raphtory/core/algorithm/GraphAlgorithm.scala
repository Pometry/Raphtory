package com.raphtory.core.algorithm

import com.typesafe.scalalogging.Logger
import net.openhft.hashing.LongHashFunction
import org.slf4j.LoggerFactory

abstract class GraphAlgorithm extends Serializable {
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def apply(graph: GraphPerspective): GraphPerspective =
    graph

  def tabularise(graph: GraphPerspective): Table =
    graph.select(vertex => Row())

  def run(graph: GraphPerspective): Table = tabularise(apply(graph))

  def ->(graphAlgorithm: GraphAlgorithm): Chain =
    Chain(this, graphAlgorithm)

  final def checkID(uniqueChars: String): Long = LongHashFunction.xx3().hashChars(uniqueChars)
}
