package com.raphtory.core.algorithm

import com.raphtory.core.client.QueryBuilder
import com.raphtory.core.graph.visitor.Vertex

/**
  * @DoNotDocument
  */
abstract class DefaultGraphOperations[G <: GraphOperations[G]](
    val queryBuilder: QueryBuilder
) extends GraphOperations[G] {
  override def setGlobalState(f: GraphState => Unit): G = addFunction(Setup(f))

  override def filter(f: (Vertex) => Boolean): G = addFunction(VertexFilter(f))

  override def filter(f: (Vertex, GraphState) => Boolean): G =
    addFunction(VertexFilterWithGraph(f))

  override def step(f: (Vertex) => Unit): G = addFunction(Step(f))

  override def step(f: (Vertex, GraphState) => Unit): G =
    addFunction(StepWithGraph(f))

  override def iterate(
      f: (Vertex) => Unit,
      iterations: Int,
      executeMessagedOnly: Boolean
  ): G = addFunction(Iterate(f, iterations, executeMessagedOnly))

  override def iterate(
      f: (Vertex, GraphState) => Unit,
      iterations: Int,
      executeMessagedOnly: Boolean
  ): G = addFunction(IterateWithGraph(f, iterations, executeMessagedOnly))

  override def select(f: Vertex => Row): Table =
    new GenericTable(queryBuilder.addGraphFunction(Select(f)))

  override def select(f: (Vertex, GraphState) => Row): Table =
    new GenericTable(queryBuilder.addGraphFunction(SelectWithGraph(f)))

  override def globalSelect(f: GraphState => Row): Table =
    new GenericTable(queryBuilder.addGraphFunction(GlobalSelect(f)))

  override def explodeSelect(f: Vertex => List[Row]): Table =
    new GenericTable(queryBuilder.addGraphFunction(ExplodeSelect(f)))

  override def clearMessages(): G =
    addFunction(ClearChain())

  private def addFunction(function: GraphFunction) =
    newGraph(queryBuilder.addGraphFunction(function))

  protected def newGraph(queryBuilder: QueryBuilder): G
}
