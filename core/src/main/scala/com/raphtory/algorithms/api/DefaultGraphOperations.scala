package com.raphtory.algorithms.api

import com.raphtory.client.QuerySender
import com.raphtory.components.querymanager.Query
import com.raphtory.graph.visitor.InterlayerEdge
import com.raphtory.graph.visitor.PropertyMergeStrategy.PropertyMerge
import com.raphtory.graph.visitor.Vertex

/**
  * @DoNotDocument
  */
abstract class DefaultGraphOperations[G <: GraphOperations[G]](
    private[api] val query: Query,
    private val querySender: QuerySender
) extends GraphOperations[G] {
  override def setGlobalState(f: GraphState => Unit): G = addFunction(Setup(f))

  override def filter(f: (Vertex) => Boolean): G = step(vertex => if (!f(vertex)) vertex.remove())

  override def filter(f: (Vertex, GraphState) => Boolean): G =
    step((vertex, graphState) => if (!f(vertex, graphState)) vertex.remove())

  override def multilayerView: G =
    addFunction(MultilayerView(None))

  override def multilayerView(
      interlayerEdgeBuilder: Vertex => Seq[InterlayerEdge]
  ): G =
    addFunction(MultilayerView(Some(interlayerEdgeBuilder)))

  override def reducedView: G =
    addFunction(ReduceView(None, None))

  override def reducedView(mergeStrategyMap: Map[String, PropertyMerge[_, _]]): G =
    addFunction(ReduceView(None, Some(mergeStrategyMap)))

  override def reducedView(mergeStrategy: PropertyMerge[_, _]): G =
    addFunction(ReduceView(Some(mergeStrategy), None))

  override def reducedView(
      defaultMergeStrategy: PropertyMerge[_, _],
      mergeStrategyMap: Map[String, PropertyMerge[_, _]]
  ): G =
    addFunction(ReduceView(Some(defaultMergeStrategy), Some(mergeStrategyMap)))

  override def aggregate(
      defaultMergeStrategy: PropertyMerge[_, _],
      mergeStrategyMap: Map[String, PropertyMerge[_, _]]
  ): G =
    addFunction(ReduceView(Some(defaultMergeStrategy), Some(mergeStrategyMap), aggregate = true))

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
