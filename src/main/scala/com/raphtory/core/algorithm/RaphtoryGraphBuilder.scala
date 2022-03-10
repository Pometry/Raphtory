package com.raphtory.core.algorithm

import com.raphtory.core.client.QueryBuilder
import com.raphtory.core.graph.visitor.Vertex

class RaphtoryGraphBuilder(private val queryBuilder: QueryBuilder) extends RaphtoryGraph {
  override def setGlobalState(f: GraphState => Unit): RaphtoryGraph = addFunction(Setup(f))

  override def filter(f: (Vertex) => Boolean): RaphtoryGraph = addFunction(VertexFilter(f))

  override def filter(f: (Vertex, GraphState) => Boolean): RaphtoryGraph =
    addFunction(VertexFilterWithGraph(f))

  override def step(f: (Vertex) => Unit): RaphtoryGraph = addFunction(Step(f))

  override def step(f: (Vertex, GraphState) => Unit): RaphtoryGraph =
    addFunction(StepWithGraph(f))

  override def iterate(
      f: (Vertex) => Unit,
      iterations: Int,
      executeMessagedOnly: Boolean
  ): RaphtoryGraph = addFunction(Iterate(f, iterations, executeMessagedOnly))

  override def iterate(
      f: (Vertex, GraphState) => Unit,
      iterations: Int,
      executeMessagedOnly: Boolean
  ): RaphtoryGraph = addFunction(IterateWithGraph(f, iterations, executeMessagedOnly))

  override def select(f: Vertex => Row): Table =
    new GenericTable(queryBuilder.addGraphFunction(Select(f)))

  override def select(f: (Vertex, GraphState) => Row): Table =
    new GenericTable(queryBuilder.addGraphFunction(SelectWithGraph(f)))

  override def globalSelect(f: GraphState => Row): Table =
    new GenericTable(queryBuilder.addGraphFunction(GlobalSelect(f)))

  override def explodeSelect(f: Vertex => List[Row]): Table =
    new GenericTable(queryBuilder.addGraphFunction(ExplodeSelect(f)))

  override def clearMessages(): RaphtoryGraph =
    addFunction(ClearChain())

  override def transform(algorithm: GraphAlgorithm): RaphtoryGraph = {
    val graph = new GenericGraphPerspective(this)
    algorithm.apply(graph).asInstanceOf[GenericGraphPerspective].graphSet
  }

  override def execute(algorithm: GraphAlgorithm): Table =
    algorithm.run(new GenericGraphPerspective(this))

  private def addFunction(function: GraphFunction) =
    new RaphtoryGraphBuilder(queryBuilder.addGraphFunction(function))
}
