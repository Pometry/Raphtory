package com.raphtory.algorithms.api.algorithm

import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.MultilayerGraphPerspective
import com.raphtory.algorithms.api.ReducedGraphPerspective
import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.Table
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

/** Base class for writing graph algorithms
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
trait GenericAlgorithm extends GenericallyApplicableAlgorithm {
  override type Out = GraphPerspective

  case class ChainedGenericAlgorithm(first: GenericAlgorithm, second: GenericAlgorithm)
          extends ChainedAlgorithm(first, second)
          with GenericAlgorithm {

    override def apply(graph: GraphPerspective): graph.Graph =
      second(first(graph).clearMessages())

    override def tabularise(graph: GraphPerspective): Table =
      second.tabularise(graph)
  }

  case class ChainedMultilayerProjectionAlgorithm(
      first: GenericAlgorithm,
      second: MultilayerProjectionAlgorithm
  ) extends ChainedAlgorithm(first, second)
          with MultilayerProjectionAlgorithm {

    override def apply(graph: GraphPerspective): graph.MultilayerGraph =
      second(first(graph).clearMessages())

    override def tabularise(graph: MultilayerGraphPerspective): Table =
      second.tabularise(graph)
  }

  case class ChainedGenericReductionAlgorithm(
      first: GenericAlgorithm,
      second: GenericReductionAlgorithm
  ) extends ChainedAlgorithm(first, second)
          with GenericReductionAlgorithm {

    override def apply(graph: GraphPerspective): graph.ReducedGraph =
      second(first(graph).clearMessages())

    override def tabularise(graph: ReducedGraphPerspective): Table =
      second.tabularise(graph)
  }

  /** Logger instance for writing out log messages */
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  /** Default implementation returns the graph unchanged
    *
    * @param graph graph to run function upon
    */
  def apply(graph: GraphPerspective): graph.Graph =
    graph.identity

  def tabularise(graph: GraphPerspective): Table =
    graph.globalSelect(_ => Row())

  def run(graph: GraphPerspective): Table = tabularise(apply(graph))

  override def ->(graphAlgorithm: GenericAlgorithm): GenericAlgorithm =
    ChainedGenericAlgorithm(this, graphAlgorithm)

  def ->(graphAlgorithm: MultilayerProjectionAlgorithm): MultilayerProjectionAlgorithm =
    ChainedMultilayerProjectionAlgorithm(this, graphAlgorithm)

  def ->(graphAlgorithm: GenericReductionAlgorithm): GenericReductionAlgorithm =
    ChainedGenericReductionAlgorithm(this, graphAlgorithm)

}
