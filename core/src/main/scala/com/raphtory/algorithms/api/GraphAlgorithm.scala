package com.raphtory.algorithms.api

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

/**
  * `GraphAlgorithm`
  *  : Base class for writing graph algorithms
  *
  * ## Attributes
  *
  *  `logger: Logger`
  *    : Logger instance for writing out log messages
  *
  * ## Methods
  *
  *  `apply(graph: GraphPerspective): GraphPerspective`
  *    : Default implementation returns the graph unchanged
  *
  *   `tabularise(graph: GraphPerspective): Table`
  *    : Return tabularised results (default implementation returns empty table)
  *
  *   `run(graph: GraphPerspective): Unit`
  *      : Run graph algorithm and output results (called internally by the query API to execute the algorithm).
  *        Normally, this method should not be overriden. Overriding this method can mean that the algorithm will
  *        behave differently, depending on whether it is called as part of a [](com.raphtory.algorithms.api.Chain)
  *        or on its own.
  *
  *   `->(graphAlgorithm: GraphAlgorithm): Chain`
  *      : Create a new algorithm [](com.raphtory.algorithms.api.Chain) which runs this algorithm first before
  *        running the other algorithm.
  *
  *        `graphAlgorithm: GraphAlgorithm)`
  *          : next algorithm to run in the chain
  */
abstract class GraphAlgorithm extends Serializable {
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def apply(graph: GraphPerspective): GraphPerspective =
    graph

  def tabularise(graph: GraphPerspective): Table =
    graph.select(vertex => Row())

  def run(graph: GraphPerspective): Table = tabularise(apply(graph))

  def ->(graphAlgorithm: GraphAlgorithm): Chain =
    Chain(this, graphAlgorithm)
}
