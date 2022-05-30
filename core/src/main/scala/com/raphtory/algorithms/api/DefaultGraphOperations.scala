package com.raphtory.algorithms.api

import com.raphtory.client.QuerySender
import com.raphtory.components.querymanager.Query
import com.raphtory.graph.visitor.InterlayerEdge
import com.raphtory.graph.visitor.PropertyMergeStrategy.PropertyMerge
import com.raphtory.graph.visitor
import com.raphtory.graph.visitor.Edge

/**
  * @note DoNotDocument
  */
trait DefaultGraphOperations[
    V <: visitor.Vertex,
    G <: DefaultGraphOperations[V, G, RG, MG],
    RG <: DefaultGraphOperations[visitor.Vertex, RG, RG, MG],
    MG <: DefaultMultilayerGraphOperations[MG, RG]
] extends ConcreteGraphPerspective[V, G, RG, MG] { this: G =>
  private[api] val query: Query
  private[api] val querySender: QuerySender

  override def setGlobalState(f: GraphState => Unit): G = addFunction(SetGlobalState(f))

  override def vertexFilter(f: (V) => Boolean): G =
    step(vertex => if (!f(vertex)) vertex.remove())

  override def vertexFilter(f: (V, GraphState) => Boolean): G =
    step((vertex: V, graphState) => if (!f(vertex, graphState)) vertex.remove())

  override def multilayerView: MG =
    addMFunction(MultilayerView(None))

  override def multilayerView(
      interlayerEdgeBuilder: visitor.Vertex => Seq[InterlayerEdge]
  ): MG =
    addMFunction(MultilayerView(Some(interlayerEdgeBuilder)))

  override def reducedView: RG =
    addRFunction(ReduceView(None, None))

  override def reducedView(
      mergeStrategyMap: Map[String, PropertyMerge[_, _]]
  ): RG =
    addRFunction(ReduceView(None, Some(mergeStrategyMap)))

  override def reducedView(mergeStrategy: PropertyMerge[_, _]): RG =
    addRFunction(ReduceView(Some(mergeStrategy), None))

  override def reducedView(
      defaultMergeStrategy: PropertyMerge[_, _],
      mergeStrategyMap: Map[String, PropertyMerge[_, _]]
  ): RG =
    addRFunction(ReduceView(Some(defaultMergeStrategy), Some(mergeStrategyMap)))

  override def aggregate(
      defaultMergeStrategy: PropertyMerge[_, _],
      mergeStrategyMap: Map[String, PropertyMerge[_, _]]
  ): RG =
    addRFunction(ReduceView(Some(defaultMergeStrategy), Some(mergeStrategyMap), aggregate = true))

  override def edgeFilter(f: Edge => Boolean, pruneNodes: Boolean): G = {
    val filtered = step { vertex =>
      vertex
        .getOutEdges()
        .foreach(edge => if (!f(edge)) edge.remove())
    }
    if (pruneNodes)
      filtered.vertexFilter((vertex: V) => vertex.degree > 0)
    else filtered
  }

  override def edgeFilter(f: (Edge, GraphState) => Boolean, pruneNodes: Boolean): G = {
    val filtered = step((vertex, graphState) =>
      vertex
        .getOutEdges()
        .foreach(edge => if (!f(edge, graphState)) edge.remove())
    )
    if (pruneNodes)
      filtered.vertexFilter(vertex => vertex.degree > 0)
    else filtered
  }

  override def step(f: (V) => Unit): G = addFunction(Step(f))

  override def step(f: (V, GraphState) => Unit): G =
    addFunction(StepWithGraph(f))

  override def iterate(
      f: (V) => Unit,
      iterations: Int,
      executeMessagedOnly: Boolean
  ): G = addFunction(Iterate(f, iterations, executeMessagedOnly))

  override def iterate(
      f: (V, GraphState) => Unit,
      iterations: Int,
      executeMessagedOnly: Boolean
  ): G = addFunction(IterateWithGraph(f, iterations, executeMessagedOnly))

  override def select(f: V => Row): Table =
    addSelect(Select(f))

  override def select(f: (V, GraphState) => Row): Table =
    addSelect(SelectWithGraph(f))

  override def globalSelect(f: GraphState => Row): Table =
    addSelect(GlobalSelect(f))

  override def explodeSelect(f: V => List[Row]): Table =
    addSelect(ExplodeSelect(f))

  override def clearMessages(): G =
    addFunction(ClearChain())

  private def addFunction(function: GraphFunction) =
    newGraph(query.copy(graphFunctions = query.graphFunctions.enqueue(function)), querySender)

  private def addRFunction(function: GraphFunction) =
    newRGraph(query.copy(graphFunctions = query.graphFunctions.enqueue(function)), querySender)

  private def addMFunction(function: GraphFunction) =
    newMGraph(query.copy(graphFunctions = query.graphFunctions.enqueue(function)), querySender)

  private def addSelect(function: GraphFunction) =
    new GenericTable(
            query.copy(graphFunctions = query.graphFunctions.enqueue(function)),
            querySender
    )

  protected def newGraph(query: Query, querySender: QuerySender): G
  protected def newRGraph(query: Query, querySender: QuerySender): RG
  protected def newMGraph(query: Query, querySender: QuerySender): MG
}

trait DefaultMultilayerGraphOperations[
    G <: DefaultMultilayerGraphOperations[G, RG],
    RG <: DefaultGraphOperations[visitor.Vertex, RG, RG, G]
] extends DefaultGraphOperations[visitor.ExplodedVertex, G, RG, G]
        with ConcreteMultilayerGraphPerspective[G, RG] { this: G => }
