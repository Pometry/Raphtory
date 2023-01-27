package com.raphtory.api.analysis.graphview

import com.raphtory.api.analysis.graphstate.GraphState
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table
import com.raphtory.api.analysis.visitor
import com.raphtory.api.analysis.visitor.Edge
import com.raphtory.api.analysis.visitor.ExplodedVertex
import com.raphtory.api.analysis.visitor.InterlayerEdge
import com.raphtory.api.analysis.visitor.PropertyMergeStrategy
import PropertyMergeStrategy.PropertyMerge

import scala.jdk.CollectionConverters.ListHasAsScala

/** Public interface for graph operations
  *
  * The `GraphPerspective` is the interface for defining algorithms in Raphtory and records all operations to be
  * applied to a graph as a sequence of steps to execute.
  *
  * Most operations on a `GraphPerspective` return a graph or multilayer
  * graph as a result. To support returning the
  * correct graph type, the `GraphPerspective` has abstract type members
  *
  *  - `Graph`: Current graph type (can be multilayer or reduced)
  *  - `MultilayerGraph`: The type of the [[MultilayerGraphPerspective]] corresponding to the current graph
  *  - `ReducedGraph`: The type of the [[ReducedGraphPerspective]] corresponding to the current graph
  */
trait GraphPerspective {

  /** The actual vertex type of this graph */
  type Vertex <: visitor.Vertex

  /** The actual type of this graph */
  type Graph <: ConcreteGraphPerspective[Vertex, Graph, ReducedGraph, MultilayerGraph]

  /** Return type for calling `reducedView` on this graph */
  type ReducedGraph <: ConcreteReducedGraphPerspective[
          ReducedGraph,
          MultilayerGraph
  ]

  /** Return type for calling `multilayerView` on this graph */
  type MultilayerGraph <: ConcreteMultilayerGraphPerspective[
          MultilayerGraph,
          ReducedGraph
  ]

  /**
    * Return the graph unchanged
    *
    * This is used to obtain the correct return type without performing any operations
    */
  def identity: Graph

  /** Add a function to manipulate global graph state, mainly used to initialise accumulators before the next algorithm step
    *
    * @param f function to set graph state (runs exactly once)
    */
  def setGlobalState(f: (GraphState) => Unit): Graph

  /** Filter vertices of the graph
    *
    * @param f filter function (only vertices for which `f` returns `true` are kept). All associated edges of filtered vertices are also removed.
    */
  def vertexFilter(f: (Vertex) => Boolean): Graph

  /** Filter vertices of the graph with global graph state
    *
    * @param f filter function with access to graph state (only vertices for which `f` returns `true` are kept). All associated edges of filtered vertices are also removed.
    */
  def vertexFilter(f: (Vertex, GraphState) => Boolean): Graph

  /** Filter edges of the graph
    *
    * @param f filter function (only edges for which `f` returns `true` are kept)
    *
    * @param pruneNodes if this is `true` then vertices which become isolated (have no incoming or outgoing edges)
    *                   after this filtering are also removed.
    */
  def edgeFilter(f: (Edge) => Boolean, pruneNodes: Boolean): Graph

  /** Filter edges of the graph with global graph state
    *
    * @param f filter function with access to graph state (only edges for which `f` returns `true` are kept)
    *
    * @param pruneNodes if this is `true` then vertices which become isolated (have no incoming or outgoing edges)
    *                   after this filtering are also removed.
    */
  def edgeFilter(f: (Edge, GraphState) => Boolean, pruneNodes: Boolean): Graph

  /** Switch to multilayer view
    *
    * After calling `multilayerView`, subsequent methods that manipulate vertices act on
    * [[visitor.ExplodedVertex ExplodedVertices]] instead. If [[visitor.ExplodedVertex ExplodedVertex]]
    * instances were already created by a previous call to `multilayerView`, they are preserved. Otherwise, this
    * method creates an [[visitor.ExplodedVertex ExplodedVertex]] instance for each
    * time point that a vertex is active.
    */
  def multilayerView: MultilayerGraph

  /** Switch to multilayer view and add interlayer edges.
    *
    * @param interlayerEdgeBuilder Interlayer edge builder to create interlayer edges for each vertex. See
    *                              [[visitor.InterlayerEdgeBuilders InterlayerEdgeBuilders]] for predefined options.
    *
    * Existing [[visitor.ExplodedVertex ExplodedVertex]] instances are preserved but all interlayer edges are recreated using the supplied
    * `interlayerEdgeBuilder`.
    */
  def multilayerView(
      interlayerEdgeBuilder: visitor.Vertex => Seq[InterlayerEdge] = _ => Seq()
  ): MultilayerGraph

  /** Switch computation to act on [[visitor.Vertex Vertex]], inverse of `multilayerView`
    *
    * This operation does nothing if the view is already reduced. Otherwise it switches back to running computations
    * on vertices but preserves any existing [[visitor.ExplodedVertex ExplodedVertex]] instances
    * created by previous calls to `multilayerView` to allow switching back-and-forth between views while
    * preserving computational state. Computational state on [[visitor.ExplodedVertex ExplodedVertex]]
    * instances is not accessible from the `Vertex` unless a merge strategy is supplied (see below).
    */
  def reducedView: ReducedGraph

  /** Reduce view and apply the same merge strategy to convert each exploded state to vertex state
    *
    * @param mergeStragegy Function to convert a history of values of type to a single value of type (see
    *          [[PropertyMergeStrategy]] for predefined options)
    */
  def reducedView(mergeStrategy: PropertyMerge[_, _]): ReducedGraph

  /** Reduce view and merge selected exploded state to vertex state
    *
    * @param mergeStrategyMap Map from state key to merge strategy. Only state included in
    *                         `mergeStrategyMap` will be reduced and
    *                         made available on the Vertex.
    */
  def reducedView(
      mergeStrategyMap: Map[String, PropertyMerge[_, _]]
  ): ReducedGraph

  /** Reduce view and merge all exploded vertex state
    *
    * @param defaultMergeStrategy Merge strategy for state not included in `mergeStrategyMap`
    *
    * @param mergeStrategyMap Map from state key to merge strategy (used to override `defaultMergeStrategy`).
    */
  def reducedView(
      defaultMergeStrategy: PropertyMerge[_, _],
      mergeStrategyMap: Map[String, PropertyMerge[_, _]]
  ): ReducedGraph

  /** Reduce view and delete exploded vertices permanently
    *
    * This function has the same effect as `reducedView`, except that the exploded vertices are deleted and no longer
    * available for subsequent calls of `multilayerView`.
    *
    * @param defaultMergeStrategy Merge strategy for state not included in `mergeStrategyMap`
    *
    * @param mergeStrategyMap Map from state key to merge strategy (used to override `defaultMergeStrategy`).
    */
  def aggregate(
      defaultMergeStrategy: PropertyMerge[_, _] = PropertyMergeStrategy.sequence[Any],
      mergeStrategyMap: Map[String, PropertyMerge[_, _]] = Map.empty[String, PropertyMerge[_, _]]
  ): ReducedGraph

  /**
    * View the graph as an undirected network (edges that exist only in one direction are duplicated and edges that exist
    * in both directions are merged)
    */
  def undirectedView: Graph

  /**
    * View the graph as a directed network (duplicated edges created by a previous call to `undirectedView` are removed
    * and bi-directional edges un-merged)
    */
  def directedView: Graph

  /**
    * Reverse the direction of each edge in the graph.
    *
    * Note that if the graph is currently viewed as undirected, this method has no immediate effect, however,
    * edges will be reversed on a subsequent call to `directedView`
    */
  def reversedView: Graph

  /** Execute algorithm step
    *
    * @param f algorithm step (run once for each vertex)
    */
  def step(f: (Vertex) => Unit): Graph

  /** Execute algorithm step with global graph state (has access to accumulated state from
    * previous steps and allows for accumulation of new values)
    *
    * @param f algorithm step (run once for each vertex)
    */
  def step(f: (Vertex, GraphState) => Unit): Graph

  /** Execute algorithm step
    *
    * @param f algorithm step (run on each edge)
    */
  def edgeStep(f: (Edge) => Unit): Graph

  /** Execute algorithm step with global graph state (has access to accumulated state from
    * previous steps and allows for accumulation of new values)
    *
    * @param f algorithm step (run on each edge)
    */
  def edgeStep(f: (Edge, GraphState) => Unit): Graph

  /** Execute algorithm step repeatedly for given number of iterations or until all vertices have voted to halt.
    *
    * @param f algorithm step (run once for each vertex per iteration)Ï
    *
    * @param iterations maximum number of iterations
    *
    * @param executeMessagedOnly If `true`, only run step for vertices which received new messages
    */

  def iterate(f: (Vertex) => Unit, iterations: Int, executeMessagedOnly: Boolean): Graph

  /** Execute algorithm step with global graph state repeatedly for given number of iterations or
    * until all vertices have voted to halt.
    *
    * @param f algorithm step (run once for each vertex per iteration)
    *
    * @param iterations maximum number of iterations
    *
    * @param executeMessagedOnly If `true`, only run step for vertices which received new messages
    */
  def iterate(
      f: (Vertex, GraphState) => Unit,
      iterations: Int,
      executeMessagedOnly: Boolean
  ): Graph

  /** Returns a table with one row per vertex and one column per key in `values`
    *
    * If no values are provided, all of the values from the properties and the state are extracted.
    *
    * @param values the keys to be extracted from every vertex state and properties
    */
  def select(values: String*): Table

  /** Returns a table with a single row from the global grah state and one column per key in `values`
    *
    * @param values the keys to be extracted from the graph state
    */
  def globalSelect(values: String*): Table

  /** Clear messages from previous operations. This function is predominantly used internally and shouldn't be called by the user unless they know what they are doing. */
  def clearMessages(): Graph
}

/** GraphPerspective over `ExplodedVertex` instances */
trait MultilayerGraphPerspective extends GraphPerspective {
  override type Vertex          = ExplodedVertex
  override type Graph <: ConcreteMultilayerGraphPerspective[Graph, ReducedGraph]
  override type MultilayerGraph = Graph
}

/** GraphPerspective over `Vertex` instances */
trait ReducedGraphPerspective extends GraphPerspective {
  override type Vertex       = visitor.ReducedVertex
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

  // TODO: Duplicate method definitions below are a work around for IntelliJ analyser bug:
  // https://youtrack.jetbrains.com/issue/SCL-20292/Combining-dependent-types-and-self-type-annotations-causes-spurious-Type-Mismatch-highlights
  //
  // Once this bug is fixed, these definitions should be removed again.

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
  def undirectedView: Graph
  def directedView: Graph
  def reversedView: Graph
  def step(f: (Vertex) => Unit): Graph
  def step(f: (Vertex, GraphState) => Unit): Graph
  def edgeStep(f: Edge => Unit): Graph
  def edgeStep(f: (Edge, GraphState) => Unit): Graph
  def iterate(f: (Vertex) => Unit, iterations: Int, executeMessagedOnly: Boolean): Graph

  def iterate(
      f: (Vertex, GraphState) => Unit,
      iterations: Int,
      executeMessagedOnly: Boolean
  ): Graph
  def select(values: String*): Table
  def globalSelect(values: String*): Table
  def clearMessages(): Graph
}

private[api] trait ConcreteReducedGraphPerspective[
    G <: ConcreteReducedGraphPerspective[G, MG],
    MG <: ConcreteMultilayerGraphPerspective[MG, G]
] extends ReducedGraphPerspective
        with ConcreteGraphPerspective[visitor.ReducedVertex, G, G, MG] { this: G => }

private[api] trait ConcreteMultilayerGraphPerspective[
    G <: ConcreteMultilayerGraphPerspective[G, RG],
    RG <: ConcreteReducedGraphPerspective[RG, G]
] extends MultilayerGraphPerspective
        with ConcreteGraphPerspective[ExplodedVertex, G, RG, G] { this: G => }
