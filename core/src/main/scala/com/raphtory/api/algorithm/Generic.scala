package com.raphtory.api.algorithm

import com.raphtory.api.graphview.GraphPerspective
import com.raphtory.api.graphview.MultilayerGraphPerspective
import com.raphtory.api.graphview.ReducedGraphPerspective
import com.raphtory.api.table.Row
import com.raphtory.api.table.Table
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
trait Generic extends GenericallyApplicable {
  override type Out = GraphPerspective

  case class ChainedGeneric(first: Generic, second: Generic)
          extends ChainedAlgorithm(first, second)
          with Generic {

    override def apply(graph: GraphPerspective): graph.Graph =
      second(first(graph).clearMessages())

    override def tabularise(graph: GraphPerspective): Table =
      second.tabularise(graph)
  }

  case class ChainedMultilayerProjection(
      first: Generic,
      second: MultilayerProjection
  ) extends ChainedAlgorithm(first, second)
          with MultilayerProjection {

    override def apply(graph: GraphPerspective): graph.MultilayerGraph =
      second(first(graph).clearMessages())

    override def tabularise(graph: MultilayerGraphPerspective): Table =
      second.tabularise(graph)
  }

  case class ChainedGenericReduction(
      first: Generic,
      second: GenericReduction
  ) extends ChainedAlgorithm(first, second)
          with GenericReduction {

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

  override def ->(graphAlgorithm: Generic): Generic =
    ChainedGeneric(this, graphAlgorithm)

  def ->(graphAlgorithm: MultilayerProjection): MultilayerProjection =
    ChainedMultilayerProjection(this, graphAlgorithm)

  def ->(graphAlgorithm: GenericReduction): GenericReduction =
    ChainedGenericReduction(this, graphAlgorithm)

}
