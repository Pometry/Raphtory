package com.raphtory.api.analysis.algorithm

import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table

private[api] trait BaseAlgorithm[T] extends Serializable {

  /** Input graph type */
  type In <: GraphPerspective

  /** Output graph type */
  type Out <: GraphPerspective

  /** Apply this algorithm to a graph, returning a transformed graph
    *
    * This method should be overidden by concrete algorithm implementations.
    */
  def apply(graph: In): Out

  /** Write out results to a table (this method is only called after `apply`)
    *
    * The default implementation returns an empty table
    *
    * @param graph Graph that results from the call to `apply`
    */
  def tabularise(graph: Out): Table[T] = graph.globalSelect(_ => Row().asInstanceOf[T]) // TODO: have a thought

  private[raphtory] def run(graph: In): Table[T] = tabularise(apply(graph))

  /** The name of the algorithm (returns the simple class name by default) */
  def name: String = {
    val name = getClass.getSimpleName
    name.stripSuffix("$")
  }

  /** Create a new algorithm which runs this algorithm first before
    *  running the other algorithm.
    *
    *  @param other next algorithm to run
    */
  def ->[Q](other: Generic[Q]): BaseAlgorithm[Q]
}

/** Trait that is extended by all algorithms that can be applied to any graph view */
trait GenericallyApplicable[T] extends BaseAlgorithm[T] {
  override type In = GraphPerspective
}

abstract private class ChainedAlgorithm[T, Q, A <: BaseAlgorithm[T], B <: BaseAlgorithm[Q]](
    val first: A,
    val second: B
) extends BaseAlgorithm[Q] {
  override def name: String = first.name + ":" + second.name
}
