package com.raphtory.algorithms.api

import com.raphtory.components.querymanager.QueryManagement
import com.raphtory.graph.visitor.Edge
import com.raphtory.graph.visitor.InterlayerEdge
import com.raphtory.graph.visitor.PropertyMergeStrategy
import com.raphtory.graph.visitor.Vertex
import PropertyMergeStrategy.PropertyMerge

sealed trait GraphFunction                             extends QueryManagement
final case class SetGlobalState(f: GraphState => Unit) extends GraphFunction

final case class MultilayerView(
    interlayerEdgeBuilder: Option[Vertex => Seq[InterlayerEdge]]
) extends GraphFunction

final case class ReduceView(
    defaultMergeStrategy: Option[PropertyMerge[_, _]],
    mergeStrategyMap: Option[Map[String, PropertyMerge[_, _]]],
    aggregate: Boolean = false
) extends GraphFunction

final case class Step(f: (Vertex) => Unit) extends GraphFunction

final case class StepWithGraph(
    f: (Vertex, GraphState) => Unit,
    graphState: GraphStateImplementation = GraphStateImplementation.empty
) extends GraphFunction

final case class Iterate(f: (Vertex) => Unit, iterations: Int, executeMessagedOnly: Boolean)
        extends GraphFunction

final case class IterateWithGraph(
    f: (Vertex, GraphState) => Unit,
    iterations: Int,
    executeMessagedOnly: Boolean,
    graphState: GraphStateImplementation = GraphStateImplementation.empty
) extends GraphFunction

final case class Select(f: Vertex => Row) extends GraphFunction

final case class SelectWithGraph(
    f: (Vertex, GraphState) => Row,
    graphState: GraphStateImplementation = GraphStateImplementation.empty
) extends GraphFunction

final case class GlobalSelect(
    f: GraphState => Row,
    graphState: GraphStateImplementation = GraphStateImplementation.empty
)                                                      extends GraphFunction
final case class ExplodeSelect(f: Vertex => List[Row]) extends GraphFunction
final case class ClearChain()                          extends GraphFunction
final case class PerspectiveDone()                     extends GraphFunction

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
trait GraphOperations[G <: GraphOperations[G]] {
  def setGlobalState(f: (GraphState) => Unit): G
  def vertexFilter(f: (Vertex) => Boolean): G
  def vertexFilter(f: (Vertex, GraphState) => Boolean): G
  def edgeFilter(f: (Edge) => Boolean, pruneNodes: Boolean): G
  def edgeFilter(f: (Edge, GraphState) => Boolean, pruneNodes: Boolean): G

  def multilayerView: G

  def multilayerView(
      interlayerEdgeBuilder: Vertex => Seq[InterlayerEdge] = _ => Seq()
  ): G

  def reducedView: G

  def reducedView(mergeStrategy: PropertyMerge[_, _]): G

  def reducedView(mergeStrategyMap: Map[String, PropertyMerge[_, _]]): G

  def reducedView(
      defaultMergeStrategy: PropertyMerge[_, _],
      mergeStrategyMap: Map[String, PropertyMerge[_, _]]
  ): G

  def aggregate(
      defaultMergeStrategy: PropertyMerge[_, _] = PropertyMergeStrategy.sequence[Any],
      mergeStrategyMap: Map[String, PropertyMerge[_, _]] = Map.empty[String, PropertyMerge[_, _]]
  ): G

  def step(f: (Vertex) => Unit): G
  def step(f: (Vertex, GraphState) => Unit): G
  def iterate(f: (Vertex) => Unit, iterations: Int, executeMessagedOnly: Boolean): G

  def iterate(
      f: (Vertex, GraphState) => Unit,
      iterations: Int,
      executeMessagedOnly: Boolean
  ): G
  def select(f: Vertex => Row): Table
  def select(f: (Vertex, GraphState) => Row): Table
  def globalSelect(f: GraphState => Row): Table
  def explodeSelect(f: Vertex => List[Row]): Table
  def clearMessages(): G
}
