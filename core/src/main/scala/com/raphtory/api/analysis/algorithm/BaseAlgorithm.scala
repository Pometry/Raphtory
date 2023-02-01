package com.raphtory.api.analysis.algorithm

import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

private[raphtory] trait BaseAlgorithm extends Serializable {

  /** Logger instance for writing out log messages */

  private var internalLogger: Logger = _

  protected def logger =
    if (internalLogger == null) {
      internalLogger = Logger(LoggerFactory.getLogger(this.getClass))
      internalLogger
    }
    else internalLogger

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
  def tabularise(graph: Out): Table = graph.globalSelect()

  /** Apply the algorithm to the graph and return the output as a table using the `tabularise` method */
  final def run(graph: In): Table = tabularise(apply(graph))

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
  def ->(other: Generic): BaseAlgorithm
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
