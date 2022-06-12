package com.raphtory.filters

import com.raphtory.algorithms.generic.NodeList
import com.raphtory.api.analysis.graphview.GraphPerspective

import scala.util.Random

/**
  * {s} `UniformVertexSample(p:Float)`
  *   : Filtered view of the graph achieved by taking a uniform random sample of the vertices
  *
  *   This retains each vertex of the graph with probability p, and edges between those retained vertices.
  *   Also known as induced subgraph sampling.
  *
  * ```{seealso}
  * [](com.raphtory.filters.VertexFilter)
  * [](com.raphtory.filters.UniformEdgeSample)
  * ```
  */

class UniformVertexSample(p: Float) extends NodeList() {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph.vertexFilter(_ => Random.nextFloat() < p)

}

object UniformVertexSample {
  def apply(p: Float) = new UniformVertexSample(p)
}
