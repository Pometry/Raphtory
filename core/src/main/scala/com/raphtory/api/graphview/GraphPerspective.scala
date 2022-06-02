package com.raphtory.api.graphview

import com.raphtory.api.graphstate.GraphState
import com.raphtory.api.graphstate.GraphStateImplementation
import com.raphtory.api.visitor
import com.raphtory.api.table.Row
import com.raphtory.api.table.Table
import com.raphtory.api.visitor.Edge
import com.raphtory.api.visitor.ExplodedVertex
import com.raphtory.api.visitor.InterlayerEdge
import com.raphtory.api.visitor.PropertyMergeStrategy
import com.raphtory.api.visitor.PropertyMergeStrategy.PropertyMerge
import com.raphtory.api.visitor.Vertex
import com.raphtory.components.querymanager.QueryManagement

/**
  * `GraphOperations[G <: GraphOperations[G]]`
  *  : Public interface for graph operations
  *
  * The `GraphOperations` interface exposes all the available operations to be executed on a graph. It returns a
  * generic type `G` for those operations that have a graph as a result.
  *
  * ## Methods
  *
  *  `setGlobalState(f: (GraphState) => Unit): G`
  *    : Add a function to manipulate global graph state, mainly used to initialise accumulators
  *       before the next algorithm step
  *
  *      `f: (GraphState) => Unit`
  *      : function to set graph state (run exactly once)
  *
  *  `filter(f: (Vertex) => Boolean): G`
  *    : Filter vertices of the graph
  *
  *      `f: (Vertex) => Boolean)`
  *      : filter function (only vertices for which `f` returns `true` are kept)
  *
  *  `filter(f: (Vertex, GraphState) => Boolean): G`
  *    : Filter vertices of the graph with global graph state
  *
  *      `f: (Vertex, GraphState) => Boolean`
  *        : filter function with access to graph state (only vertices for which `f` returns `true` are kept)
  *
  *  `edgeFilter(f: (Edge) => Boolean, pruneNodes: Boolean): G`
  *    : Filter edges of the graph
  *
  *      `f: (Edge) => Boolean`
  *        : filter function (only edges for which {s}`f` returns {s}`true` are kept)
  *
  *      `pruneNodes: Boolean`
  *        : if this is {s}`true` then vertices which become isolated (have no incoming or outgoing edges)
  *        after this filtering are also removed.
  *
  *  `edgeFilter(f: (Edge, GraphState) => Boolean, pruneNodes: Boolean): G`
  *    : Filter edges of the graph with global graph state
  *
  *      `f: (Edge, GraphState) => Boolean`
  *        : filter function with access to graph state (only edges for which {s}`f` returns {s}`true` are kept)
  *
  *      `pruneNodes: Boolean`
  *        : if this is {s}`true` then vertices which become isolated (have no incoming or outgoing edges)
  *        after this filtering are also removed.
  *
  *  `multilayerView: G`
  *    : Switch to multilayer view
  *
  *      After calling `multilayerView`, subsequent methods that manipulate vertices act on
  *      [`ExplodedVertex`](com.raphtory.graph.visitor.ExplodedVertex) instead. If [`ExplodedVertex](com.raphtory.graph.visitor.ExplodedVertex)`
  *      instances were already created by a previous call to `multilayerView`, they are preserved. Otherwise, this
  *      method creates an [`ExplodedVertex`](com.raphtory.graph.visitor.ExplodedVertex) instance for each
  *      timepoint that a vertex is active.
  *
  *  `multilayerView(interlayerEdgeBuilder: Vertex => Seq[InterlayerEdge]): G`
  *    : Switch to multilayer view and add interlayer edges.
  *
  *      `interlayerEdgeBuilder: Vertex => Seq[InterlayerEdge]`
  *        : Interlayer edge builder to create interlayer edges for each vertex. See
  *          [`InterlayerEdgeBuilders`](com.raphtory.graph.visitor.InterlayerEdgeBuilders) for predefined
  *          options.
  *
  *      Existing `ExplodedVertex` instances are preserved but all interlayer edges are recreated using the supplied
  *      `interlayerEdgeBuilder`.
  *
  *  `reducedView: G`
  *    : Switch computation to act on [`Vertex`](com.raphtory.graph.visitor.Vertex), inverse of `multilayerView`
  *
  *      This operation does nothing if the view is already reduced. Otherwise it switches back to running computations
  *      on vertices but preserves any existing [`ExplodedVertex`](com.raphtory.graph.visitor.ExplodedVertex) instances
  *      created by previous calls to `multilayerView` to allow switching back-and-forth between views while
  *      preserving computational state. Computational state on [`ExplodedVertex`](com.raphtory.graph.visitor.ExplodedVertex)
  *      instances is not accessible from the `Vertex` unless a merge strategy is supplied (see below).
  *
  *  `reducedView(mergeStrategy: PropertyMerge[_, _]): G`
  *    : Reduce view and apply the same merge strategy to convert each exploded state to vertex state
  *
  *      `mergeStragegy: PropertyMerge[_, _]`
  *        : Function to convert a history of values of type to a single value of type (see
  *          [`PropertyMergeStrategy`](com.raphtory.graph.visitor.PropertyMergeStrategy) for predefined options)
  *
  *  `reducedView(mergeStrategyMap: Map[String, PropertyMerge[_, _]]): G`
  *    : Reduce view and merge selected exploded state to vertex state
  *
  *      `mergeStrategyMap: Map[String, PropertyMerge[_, _]]`
  *        : Map from state key to merge strategy. Only state included in `mergeStrategyMap` will be reduced and
  *          made available on the Vertex.
  *
  *  `reducedView(defaultMergeStrategy: PropertyMerge[_, _], mergeStrategyMap: Map[String, PropertyMerge[_, _]]): G
  *    : Reduce view and merge all exploded vertex state
  *
  *      `defaultMergeStrategy: `PropertyMerge[_, _]`
  *        : Merge strategy for state not included in `mergeStrategyMap`
  *
  *      `mergeStrategyMap: Map[String, PropertyMerge[_, _]]`
  *        : Map from state key to merge strategy (used to override `defaultMergeStrategy`).
  *
  *  `aggregate(defaultMergeStrategy: PropertyMerge[_, _] = PropertyMergeStrategy.sequence[Any], mergeStrategyMap: Map[String, PropertyMerge[_, _]] = Map.empty[String, PropertyMerge[_, _]]): G
  *    : Reduce view and delete exploded vertices permanently
  *
  *      This function has the same effect as `reducedView`, except that the exploded vertices are deleted and no longer
  *      available for subsequent calls of `multilayerView`.
  *
  *  `step(f: (Vertex) => Unit): G`
  *    : Execute algorithm step
  *
  *      `f: (Vertex) => Unit`
  *        : algorithm step (run once for each vertex)
  *
  *  `step(f: (Vertex, GraphState) => Unit): G`
  *    : Execute algorithm step with global graph state (has access to accumulated state from
  *      previous steps and allows for accumulation of new values)
  *
  *      `f: (Vertex, GraphState) => Unit`
  *        : algorithm step (run once for each vertex)
  *
  *  `iterate(f: (Vertex) => Unit, iterations: Int, executeMessagedOnly: Boolean): GraphPerspective`
  *     : Execute algorithm step repeatedly for given number of iterations or until all vertices have
  *       voted to halt.
  *
  *       `f: (Vertex) => Unit`
  *         : algorithm step (run once for each vertex per iteration)
  *
  *       `iterations: Int`
  *         : maximum number of iterations
  *
  *       `executeMessagedOnly: Boolean`
  *         : If `true`, only run step for vertices which received new messages
  *
  *   `iterate(f: (Vertex, GraphState) => Unit, iterations: Int, executeMessagedOnly: Boolean): G`
  *     : Execute algorithm step with global graph state repeatedly for given number of iterations or
  *       until all vertices have voted to halt.
  *
  *       `f: (Vertex, GraphState) => Unit`
  *         : algorithm step (run once for each vertex per iteration)
  *
  *       `iterations: Int`
  *         : maximum number of iterations
  *
  *       `executeMessagedOnly: Boolean`
  *         : If `true`, only run step for vertices which received new messages
  *
  *  `select(f: Vertex => Row): Table`
  *     : Write output to table
  *
  *       `f: Vertex => Row`
  *         : function to extract data from vertex (run once for each vertex)
  *
  *  `select(f: (Vertex, GraphState) => Row): Table`
  *     : Write output to table with access to global graph state
  *
  *       `f: (Vertex, GraphState) => Row`
  *         : function to extract data from vertex and graph state (run once for each vertex)
  *
  *  `globalSelect(f: GraphState => Row): Table`
  *     : Write global graph state to table (this creates a table with a single row)
  *
  *       `f: GraphState => Row`
  *         : function to extract data from graph state (run only once)
  *
  *  `clearMessages(): G`
  *     : Clear messages from previous operations
  *
  * ```{seealso}
  * [](com.raphtory.algorithms.api.GraphState), [](com.raphtory.graph.visitor.Vertex)
  * ```
  */

trait GraphPerspective {
  type Vertex <: visitor.Vertex
  type Graph <: ConcreteGraphPerspective[Vertex, Graph, ReducedGraph, MultilayerGraph]

  type ReducedGraph <: ConcreteReducedGraphPerspective[
          ReducedGraph,
          MultilayerGraph
  ]

  type MultilayerGraph <: ConcreteMultilayerGraphPerspective[
          MultilayerGraph,
          ReducedGraph
  ]

  def identity: Graph
  def setGlobalState(f: (GraphState) => Unit): Graph
  def vertexFilter(f: (Vertex) => Boolean): Graph
  def vertexFilter(f: (Vertex, GraphState) => Boolean): Graph
  def edgeFilter(f: (Edge) => Boolean, pruneNodes: Boolean): Graph
  def edgeFilter(f: (Edge, GraphState) => Boolean, pruneNodes: Boolean): Graph

  def multilayerView: MultilayerGraph

  def multilayerView(
      interlayerEdgeBuilder: visitor.Vertex => Seq[InterlayerEdge] = _ => Seq()
  ): MultilayerGraph

  def reducedView: ReducedGraph

  def reducedView(mergeStrategy: PropertyMerge[_, _]): ReducedGraph

  def reducedView(
      mergeStrategyMap: Map[String, PropertyMerge[_, _]]
  ): ReducedGraph

  def reducedView(
      defaultMergeStrategy: PropertyMerge[_, _],
      mergeStrategyMap: Map[String, PropertyMerge[_, _]]
  ): ReducedGraph

  def aggregate(
      defaultMergeStrategy: PropertyMerge[_, _] = PropertyMergeStrategy.sequence[Any],
      mergeStrategyMap: Map[String, PropertyMerge[_, _]] = Map.empty[String, PropertyMerge[_, _]]
  ): ReducedGraph

  def step(f: (Vertex) => Unit): Graph
  def step(f: (Vertex, GraphState) => Unit): Graph
  def iterate(f: (Vertex) => Unit, iterations: Int, executeMessagedOnly: Boolean): Graph

  def iterate(
      f: (Vertex, GraphState) => Unit,
      iterations: Int,
      executeMessagedOnly: Boolean
  ): Graph
  def select(f: Vertex => Row): Table
  def select(f: (Vertex, GraphState) => Row): Table
  def globalSelect(f: GraphState => Row): Table
  def explodeSelect(f: Vertex => List[Row]): Table
  def clearMessages(): Graph
}

trait MultilayerGraphPerspective extends GraphPerspective {
  override type Vertex          = ExplodedVertex
  override type Graph <: ConcreteMultilayerGraphPerspective[Graph, ReducedGraph]
  override type MultilayerGraph = Graph
}

trait ReducedGraphPerspective extends GraphPerspective {
  override type Vertex       = visitor.Vertex
  override type Graph <: ConcreteReducedGraphPerspective[Graph, MultilayerGraph]
  override type ReducedGraph = Graph
}

private[api] trait ConcreteGraphPerspective[V <: visitor.Vertex, G <: ConcreteGraphPerspective[
        V,
        G,
        RG,
        MG
], RG <: ConcreteReducedGraphPerspective[RG, MG], MG <: ConcreteMultilayerGraphPerspective[MG, RG]]
        extends GraphPerspective { this: G =>
  override type Graph           = G
  override type ReducedGraph    = RG
  override type MultilayerGraph = MG
  override type Vertex          = V
  override def identity: Graph = this
}

private[api] trait ConcreteReducedGraphPerspective[
    G <: ConcreteReducedGraphPerspective[G, MG],
    MG <: ConcreteMultilayerGraphPerspective[MG, G]
] extends ReducedGraphPerspective
        with ConcreteGraphPerspective[visitor.Vertex, G, G, MG] { this: G => }

private[api] trait ConcreteMultilayerGraphPerspective[
    G <: ConcreteMultilayerGraphPerspective[G, RG],
    RG <: ConcreteReducedGraphPerspective[RG, G]
] extends MultilayerGraphPerspective
        with ConcreteGraphPerspective[ExplodedVertex, G, RG, G] { this: G => }
