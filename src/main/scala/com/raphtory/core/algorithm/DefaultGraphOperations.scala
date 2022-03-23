package com.raphtory.core.algorithm

import com.raphtory.core.client.QuerySender
import com.raphtory.core.components.querymanager.Query
import com.raphtory.core.graph.visitor.Vertex

/**
  * @DoNotDocument
  */
abstract class DefaultGraphOperations[G <: GraphOperations[G]](
    val query: Query,
    private val querySender: QuerySender
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
    addSelect(Select(f))

  override def select(f: (Vertex, GraphState) => Row): Table =
    addSelect(SelectWithGraph(f))

  override def globalSelect(f: GraphState => Row): Table =
    addSelect(GlobalSelect(f))

  override def explodeSelect(f: Vertex => List[Row]): Table =
    addSelect(ExplodeSelect(f))

  override def clearMessages(): G =
    addFunction(ClearChain())

  private def addFunction(function: GraphFunction) =
    newGraph(query.copy(graphFunctions = query.graphFunctions.enqueue(function)), querySender)

  private def addSelect(function: GraphFunction) =
    new GenericTable(
            query.copy(graphFunctions = query.graphFunctions.enqueue(function)),
            querySender
    )

  protected def newGraph(query: Query, querySender: QuerySender): G
}
