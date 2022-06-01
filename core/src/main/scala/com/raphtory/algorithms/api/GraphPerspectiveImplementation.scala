package com.raphtory.algorithms.api

import com.raphtory.algorithms.api.algorithm.BaseGraphAlgorithm
import com.raphtory.algorithms.api.algorithm.GenericAlgorithm
import com.raphtory.algorithms.api.algorithm.GenericReductionAlgorithm
import com.raphtory.algorithms.api.algorithm.GenericallyApplicableAlgorithm
import com.raphtory.algorithms.api.algorithm.MultilayerAlgorithm
import com.raphtory.algorithms.api.algorithm.MultilayerProjectionAlgorithm
import com.raphtory.algorithms.api.algorithm.MultilayerReductionAlgorithm
import com.raphtory.client.QuerySender
import com.raphtory.components.querymanager.Query
import com.raphtory.graph.visitor.InterlayerEdge
import com.raphtory.graph.visitor.PropertyMergeStrategy.PropertyMerge
import com.raphtory.graph.visitor
import com.raphtory.graph.visitor.Edge

/**
  * @note DoNotDocument
  */
private[api] trait GraphPerspectiveImplementation[
    V <: visitor.Vertex,
    G <: GraphPerspectiveImplementation[V, G, RG, MG],
    RG <: ReducedGraphPerspectiveImplementation[RG, MG],
    MG <: MultilayerGraphPerspectiveImplementation[MG, RG]
] extends ConcreteGraphPerspective[V, G, RG, MG]
        with GraphBase[G, RG, MG] { this: G =>

  private[api] val query: Query
  private[api] val querySender: QuerySender

  override def identity: G                              = this
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

  /**  Execute only the apply step of the algorithm on every perspective and returns a new RaphtoryGraph with the result.
    *  @param algorithm algorithm to apply
    */
  def transform(algorithm: GenericAlgorithm): G =
    algorithm
      .apply(withTransformedName(algorithm))
      .clearMessages()

  def transform(algorithm: MultilayerProjectionAlgorithm): MG =
    algorithm(withTransformedName(algorithm)).clearMessages()

  def transform(algorithm: GenericReductionAlgorithm): RG =
    algorithm(withTransformedName(algorithm)).clearMessages()

  /** Execute the algorithm on every perspective and returns a new `RaphtoryGraph` with the result.
    * @param algorithm to apply
    */
  def execute(algorithm: GenericallyApplicableAlgorithm): Table =
    algorithm.run(withTransformedName(algorithm))

  private def addFunction(function: GraphFunction) =
    newGraph(query.copy(graphFunctions = query.graphFunctions.enqueue(function)), querySender)

  private def addRFunction(function: GraphFunction) =
    newRGraph(query.copy(graphFunctions = query.graphFunctions.enqueue(function)), querySender)

  private def addMFunction(function: GraphFunction) =
    newMGraph(query.copy(graphFunctions = query.graphFunctions.enqueue(function)), querySender)

  private def addSelect(function: GraphFunction) =
    new TableImplementation(
            query.copy(graphFunctions = query.graphFunctions.enqueue(function)),
            querySender
    )

  private[api] def withTransformedName(algorithm: BaseGraphAlgorithm) = {
    val newName = query.name match {
      case "" => algorithm.name
      case _  => query.name + ":" + algorithm.name
    }
    newGraph(query.copy(name = newName), querySender)
  }
}

private[api] trait MultilayerGraphPerspectiveImplementation[
    G <: MultilayerGraphPerspectiveImplementation[G, RG],
    RG <: ReducedGraphPerspectiveImplementation[RG, G]
] extends GraphPerspectiveImplementation[visitor.ExplodedVertex, G, RG, G]
        with ConcreteMultilayerGraphPerspective[G, RG] { this: G =>

  def transform(algorithm: MultilayerAlgorithm): G =
    algorithm(withTransformedName(algorithm)).clearMessages()

  def transform(algorithm: MultilayerReductionAlgorithm): RG =
    algorithm(withTransformedName(algorithm)).clearMessages()

  def execute(algorithm: MultilayerAlgorithm): Table =
    algorithm.run(withTransformedName(algorithm))

  def execute(algorithm: MultilayerReductionAlgorithm): Table =
    algorithm.run(withTransformedName(algorithm))

  private[api] def newGraph(query: Query, querySender: QuerySender): G =
    newMGraph(query, querySender)
}

private[api] trait ReducedGraphPerspectiveImplementation[G <: ReducedGraphPerspectiveImplementation[
        G,
        MG
], MG <: MultilayerGraphPerspectiveImplementation[MG, G]]
        extends ConcreteReducedGraphPerspective[G, MG]
        with GraphPerspectiveImplementation[visitor.Vertex, G, G, MG] { this: G =>

  private[api] def newGraph(query: Query, querySender: QuerySender): G =
    newRGraph(query, querySender)
}

private[api] trait GraphBase[G, RG, MG] {
  private[api] def newGraph(query: Query, querySender: QuerySender): G
  private[api] def newRGraph(query: Query, querySender: QuerySender): RG
  private[api] def newMGraph(query: Query, querySender: QuerySender): MG
}
