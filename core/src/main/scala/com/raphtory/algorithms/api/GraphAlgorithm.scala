package com.raphtory.algorithms.api

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

/** Base class for writing graph algorithms
  *
  *
  *  `apply(graph: GraphPerspective): GraphPerspective`
  *    :
  *
  *   `tabularise(graph: GraphPerspective): Table`
  *    :
  *
  *   `run(graph: GraphPerspective): Unit`
  *      :
  *
  *   `->(graphAlgorithm: GraphAlgorithm): Chain`
  *      :
  *
  *        `graphAlgorithm: GraphAlgorithm)`
  *          :
  */
abstract class GraphAlgorithm extends Serializable {
  /** Logger instance for writing out log messages */
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  /** Default implementation returns the graph unchanged
   * @param graph graph to run function upon */
  def apply(graph: GraphPerspective): GraphPerspective =
    graph

  /** Return tabularised results (default implementation returns empty table)
   * @param graph graph to run function upon */
  def tabularise(graph: GraphPerspective): Table =
    graph.select(vertex => Row())

  /** Run graph algorithm and output results (called internally by the query API to execute the algorithm).
   *  Normally, this method should not be overriden. Overriding this method can mean that the algorithm will
   *  behave differently, depending on whether it is called as part of a [](com.raphtory.algorithms.api.Chain)
   *  or on its own.
   * @param graph graph to run function upon */
  def run(graph: GraphPerspective): Table = tabularise(apply(graph))

  /** Create a new algorithm [](com.raphtory.algorithms.api.Chain) which runs this algorithm first before
   *  running the other algorithm.
   *  @param graphAlgorithm next algorithm to run in the chain */
  def ->(graphAlgorithm: GraphAlgorithm): Chain =
    Chain(this, graphAlgorithm)
}
