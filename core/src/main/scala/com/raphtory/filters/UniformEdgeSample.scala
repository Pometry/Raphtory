package com.raphtory.filters

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective

import scala.util.Random

/**
  * {s} `UniformEdgeSample(p:Float, pruneNodes:Boolean=true)`
  *   : Filtered view of the graph achieved by taking a uniform random sample of the edges
  *
  *   This retains each edge of the graph with probability p. Additionally, if pruneNodes is set to true,
  *   nodes which become isolated by this edge removal are also removed.
  *
  * ```{seealso}
  * [](com.raphtory.filters.EdgeFilter)
  * [](com.raphtory.filters.UniformVertexSample)
  * ```
  */

class UniformEdgeSample(p: Float, pruneNodes: Boolean = true) extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph.edgeFilter(_ => Random.nextFloat() < p, pruneNodes = pruneNodes)
}

object UniformEdgeSample {
  def apply(p: Float, pruneNodes: Boolean = true) = new UniformEdgeSample(p, pruneNodes)
}
