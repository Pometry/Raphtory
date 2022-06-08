package com.raphtory.api.analysis.algorithm

import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.graphview.ReducedGraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table

import scala.language.existentials

private[api] trait BaseAlgorithm extends Serializable {

  /** Input graph type */
  type In <: GraphPerspective

  /** Output graph type */
  type Out <: GraphPerspective

  /** Apply this algorithm to a graph, returning a transformed graph
    *
    * This method should be overidden by concrete algorithm implementations.
    */
  def apply(graph: In): T forSome { type T <: Out }

  /** Write out results to a table (this method is only called after `apply`)
    *
    * The default implementation returns an empty table
    *
    * @param graph Graph that results from the call to `apply`
    */
  def tabularise(graph: Out): Table = graph.globalSelect(_ => Row())

  private[raphtory] def run(graph: In): Table = tabularise(apply(graph))

  /** The name of the algorithm (returns the simple class name by default) */
  def name: String = getClass.getSimpleName

  /** Create a new algorithm which runs this algorithm first before
    *  running the other algorithm.
    *
    *  @param other next algorithm to run
    */
  def ->(other: Generic): BaseAlgorithm
}

trait ConcreteAlgorithm[I <: GraphPerspective, O <: GraphPerspective] extends BaseAlgorithm {
  override type In  = I
  override type Out = O
}

/** Trait that is extended by all algorithms that can be applied to any graph view */
trait GenericallyApplicable extends BaseAlgorithm {
  override type In = GraphPerspective
}

abstract private class ChainedAlgorithm[A <: BaseAlgorithm, B <: BaseAlgorithm](
    val first: A,
    val second: B
) extends BaseAlgorithm {
  override def name: String = first.name + ":" + second.name
}
