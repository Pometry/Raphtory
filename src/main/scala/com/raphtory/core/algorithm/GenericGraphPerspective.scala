package com.raphtory.core.algorithm

import com.raphtory.core.graph.visitor.Vertex

/**
  * @DoNotDocument
  */
class GenericGraphPerspective(val graphSet: RaphtoryGraph) extends GraphPerspective {

  override def setGlobalState(f: GraphState => Unit): GraphPerspective =
    newGraph(graphSet.setGlobalState(f))

  override def filter(f: (Vertex) => Boolean): GraphPerspective = newGraph(graphSet.filter(f))

  override def filter(f: (Vertex, GraphState) => Boolean): GraphPerspective =
    newGraph(graphSet.filter(f))

  override def step(f: (Vertex) => Unit): GraphPerspective             = newGraph(graphSet.step(f))
  override def step(f: (Vertex, GraphState) => Unit): GraphPerspective = newGraph(graphSet.step(f))

  override def iterate(
      f: (Vertex) => Unit,
      iterations: Int,
      executeMessagedOnly: Boolean
  ): GraphPerspective = newGraph(graphSet.iterate(f, iterations, executeMessagedOnly))

  override def iterate(
      f: (Vertex, GraphState) => Unit,
      iterations: Int,
      executeMessagedOnly: Boolean
  ): GraphPerspective = newGraph(graphSet.iterate(f, iterations, executeMessagedOnly))

  override def select(f: Vertex => Row): Table               = graphSet.select(f)
  override def select(f: (Vertex, GraphState) => Row): Table = graphSet.select(f)
  override def globalSelect(f: GraphState => Row): Table     = graphSet.globalSelect(f)
  override def explodeSelect(f: Vertex => List[Row]): Table  = graphSet.explodeSelect(f)
  override def clearMessages(): GraphPerspective             = newGraph(graphSet.clearMessages())

  private def newGraph(graphSet: RaphtoryGraph) = new GenericGraphPerspective(graphSet)
}
