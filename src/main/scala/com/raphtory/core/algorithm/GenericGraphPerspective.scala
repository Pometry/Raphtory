package com.raphtory.core.algorithm

import com.raphtory.core.graph.visitor.Vertex

/**
  * @DoNotDocument
  */
class GenericGraphPerspective(val graphSet: GraphPerspectiveSet) extends GraphPerspective {
  override def filter(f: (Vertex) => Boolean): GraphPerspective = newGraph(graphSet.filter(f))

  override def step(f: (Vertex) => Unit): GraphPerspective = newGraph(graphSet.step(f))

  override def iterate(
      f: (Vertex) => Unit,
      iterations: Int,
      executeMessagedOnly: Boolean
  ): GraphPerspective = newGraph(graphSet.iterate(f, iterations, executeMessagedOnly))

  override def select(f: Vertex => Row): Table = graphSet.select(f)

  override def explodeSelect(f: Vertex => List[Row]): Table = graphSet.explodeSelect(f)

  override def clearMessages(): GraphPerspective = newGraph(graphSet.clearMessages())

  private def newGraph(graphSet: GraphPerspectiveSet) =
    new GenericGraphPerspective(graphSet)
}
