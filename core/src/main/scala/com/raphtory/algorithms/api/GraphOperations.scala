package com.raphtory.algorithms.api

import com.raphtory.components.querymanager.QueryManagement
import com.raphtory.graph.visitor.InterlayerEdge
import com.raphtory.graph.visitor.PropertyMergeStrategy
import com.raphtory.graph.visitor.Vertex
import PropertyMergeStrategy.PropertyMerge

sealed trait GraphFunction                    extends QueryManagement
final case class Setup(f: GraphState => Unit) extends GraphFunction

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
  * {s}`GraphOperations[G <: GraphOperations[G]]`
  *  : Public interface for graph operations
  *
  * The {s}`GraphOperations` interface exposes all the available operations to be executed on a graph. It returns a
  * generic type {s}`G` for those operations that have a graph as a result.
  *
  * ## Methods
  *
  *  {s}`setGlobalState(f: (GraphState) => Unit): G`
  *    : Add a function to manipulate global graph state, mainly used to initialise accumulators
  *       before the next algorithm step
  *
  *      {s}`f: (GraphState) => Unit`
  *      : function to set graph state (run exactly once)
  *
  *  {s}`filter(f: (Vertex) => Boolean): G`
  *    : Filter nodes
  *
  *      {s}`f: (Vertex) => Boolean)`
  *      : filter function (only vertices for which {s}`f` returns {s}`true` are kept)
  *
  *  {s}`filter(f: (Vertex, GraphState) => Boolean): G`
  *    : Filter nodes with global graph state
  *
  *      {s}`f: (Vertex, GraphState) => Boolean`
  *        : filter function with access to graph state (only vertices for which {s}`f` returns {s}`true` are kept)
  *
  *  {s}`step(f: (Vertex) => Unit): G`
  *    : Execute algorithm step
  *
  *      {s}`f: (Vertex) => Unit`
  *        : algorithm step (run once for each vertex)
  *
  *  {s}`step(f: (Vertex, GraphState) => Unit): G`
  *    : Execute algorithm step with global graph state (has access to accumulated state from
  *      previous steps and allows for accumulation of new values)
  *
  *      {s}`f: (Vertex, GraphState) => Unit`
  *        : algorithm step (run once for each vertex)
  *
  *  {s}`iterate(f: (Vertex) => Unit, iterations: Int, executeMessagedOnly: Boolean): GraphPerspective`
  *     : Execute algorithm step repeatedly for given number of iterations or until all vertices have
  *       voted to halt.
  *
  *       {s}`f: (Vertex) => Unit`
  *         : algorithm step (run once for each vertex per iteration)
  *
  *       {s}`iterations: Int`
  *         : maximum number of iterations
  *
  *       {s}`executeMessagedOnly: Boolean`
  *         : If {s}`true`, only run step for vertices which received new messages
  *
  *   {s}`iterate(f: (Vertex, GraphState) => Unit, iterations: Int, executeMessagedOnly: Boolean): G`
  *     : Execute algorithm step with global graph state repeatedly for given number of iterations or
  *       until all vertices have voted to halt.
  *
  *       {s}`f: (Vertex, GraphState) => Unit`
  *         : algorithm step (run once for each vertex per iteration)
  *
  *       {s}`iterations: Int`
  *         : maximum number of iterations
  *
  *       {s}`executeMessagedOnly: Boolean`
  *         : If {s}`true`, only run step for vertices which received new messages
  *
  *  {s}`select(f: Vertex => Row): Table`
  *     : Write output to table
  *
  *       {s}`f: Vertex => Row`
  *         : function to extract data from vertex (run once for each vertex)
  *
  *  {s}`select(f: (Vertex, GraphState) => Row): Table`
  *     : Write output to table with access to global graph state
  *
  *       {s}`f: (Vertex, GraphState) => Row`
  *         : function to extract data from vertex and graph state (run once for each vertex)
  *
  *  {s}`globalSelect(f: GraphState => Row): Table`
  *     : Write global graph state to table (this creates a table with a single row)
  *
  *       {s}`f: GraphState => Row`
  *         : function to extract data from graph state (run only once)
  *
  *  {s}`clearMessages(): G`
  *     : Clear messages from previous operations
  *
  * ```{seealso}
  * [](com.raphtory.algorithms.api.GraphState), [](com.raphtory.graph.visitor.Vertex)
  * ```
  */

/** @DoNotDocument
  *
  * Multilayer view interface (work in progress)
  *
  *  {s}`multilayerView: G`
  *    : Switch to multilayer view
  *
  *      After calling {s}`multilayerView`, subsequent methods that manipulate vertices act on
  *      [{s}`ExplodedVertex`](com.raphtory.graph.visitor.ExplodedVertex) instead. If [{s}`ExplodedVertex](com.raphtory.graph.visitor.ExplodedVertex)`
  *      instances were already created by a previous call to {s}`multilayerView`, they are preserved. Otherwise, this
  *      method creates an [{s}`ExplodedVertex`](com.raphtory.graph.visitor.ExplodedVertex) instance for each
  *      timepoint that a vertex is active.
  *
  *  {s}`multilayerView(interlayerEdgeBuilder: Vertex => Seq[InterlayerEdge]): G`
  *    : Switch to multilayer view and add interlayer edges.
  *
  *      {s}`interlayerEdgeBuilder: Vertex => Seq[InterlayerEdge]`
  *        : Interlayer edge builder to create interlayer edges for each vertex. See
  *          [{s}`InterlayerEdgeBuilders`](com.raphtory.graph.visitor.InterlayerEdgeBuilders) for predefined
  *          options.
  *
  *      Existing {s}`ExplodedVertex` instances are preserved but all interlayer edges are recreated using the supplied
  *      {s}`interlayerEdgeBuilder`.
  *
  *  {s}`reducedView: G`
  *    : Switch computation to act on [{S}`Vertex`](com.raphtory.graph.visitor.Vertex), inverse of {s}`multilayerView`
  *
  *      This operation does nothing if the view is already reduced. Otherwise it switches back to running computations
  *      on vertices but preserves any existing [{s}`ExplodedVertex`](com.raphtory.graph.visitor.ExplodedVertex) instances
  *      created by previous calls to {s}`multilayerView` to allow switching back-and-forth between views while
  *      preserving computational state. Computational state on [{s}`ExplodedVertex`](com.raphtory.graph.visitor.ExplodedVertex)
  *      instances is not accessible from the {s}`Vertex` unless a merge strategy is supplied (see below).
  *
  *  {s}`reducedView(mergeStrategy: PropertyMerge[_, _]): G`
  *    : Reduce view and apply the same merge strategy to convert each exploded state to vertex state
  *
  *      {s}`mergeStragegy: PropertyMerge[_, _]`
  *        : Function to convert a history of values of type to a single value of type (see
  *          [{s}`PropertyMergeStrategy`](com.raphtory.graph.visitor.PropertyMergeStrategy) for predefined options)
  *
  *  {s}`reducedView(mergeStrategyMap: Map[String, PropertyMerge[_, _]]): G`
  *    : Reduce view and merge selected exploded state to vertex state
  *
  *      {s}`mergeStrategyMap: Map[String, PropertyMerge[_, _]]`
  *        : Map from state key to merge strategy. Only state included in `mergeStrategyMap` will be reduced and
  *          made available on the Vertex.
  *
  *  {s}`reducedView(defaultMergeStrategy: PropertyMerge[_, _], mergeStrategyMap: Map[String, PropertyMerge[_, _]]): G
  *    : Reduce view and merge all exploded vertex state
  *
  *      {s}`defaultMergeStrategy: `PropertyMerge[_, _]`
  *        : Merge strategy for state not included in {s}`mergeStrategyMap`
  *
  *      {s}`mergeStrategyMap: Map[String, PropertyMerge[_, _]]`
  *        : Map from state key to merge strategy (used to override {s}`defaultMergeStrategy`).
  *
  *  {s}`aggregate(defaultMergeStrategy: PropertyMerge[_, _] = PropertyMergeStrategy.sequence[Any], mergeStrategyMap: Map[String, PropertyMerge[_, _]] = Map.empty[String, PropertyMerge[_, _]]): G
  *    : Reduce view and delete exploded vertices permanently
  *
  *      This function has the same effect as {s}`reducedView`, except that the exploded vertices are deleted and no longer
  *      available for subsequent calls of {s}`multilayerView`.
  */
trait GraphOperations[G <: GraphOperations[G]] {
  def setGlobalState(f: (GraphState) => Unit): G
  def filter(f: (Vertex) => Boolean): G
  def filter(f: (Vertex, GraphState) => Boolean): G

//  def multilayerView: G
//
//  def multilayerView(
//      interlayerEdgeBuilder: Vertex => Seq[InterlayerEdge] = _ => Seq()
//  ): G
//
//  def reducedView: G
//
//  def reducedView(mergeStrategy: PropertyMerge[_, _]): G
//
//  def reducedView(mergeStrategyMap: Map[String, PropertyMerge[_, _]]): G
//
//  def reducedView(
//      defaultMergeStrategy: PropertyMerge[_, _],
//      mergeStrategyMap: Map[String, PropertyMerge[_, _]]
//  ): G
//
//  def aggregate(
//      defaultMergeStrategy: PropertyMerge[_, _] = PropertyMergeStrategy.sequence[Any],
//      mergeStrategyMap: Map[String, PropertyMerge[_, _]] = Map.empty[String, PropertyMerge[_, _]]
//  ): G

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
