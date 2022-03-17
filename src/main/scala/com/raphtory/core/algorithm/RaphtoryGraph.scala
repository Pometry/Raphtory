package com.raphtory.core.algorithm

/**
  * {s}`RaphtoryGraph`
  *  : Public interface for the analysis API
  *
  * A {s}`RaphtoryGraph` is an immutable collection of views over a graph generated for Raphtory that support all graph
  * operations. It implements the operations exposed by {s}`GraphOperations` returning a new {s}`RaphtoryGraph` for
  * those operations that have a graph as a result.
  * All the operations executed over a {s}`RaphtoryGraph` get executed individually over every view of the graph in the
  * collection. Graph states refer to the state of ever single view in the collection separately.
  *
  * ## Methods
  *
  *  {s}`transform(f: RaphtoryGraph => RaphtoryGraph): RaphtoryGraph`
  *    : Apply f over itself and return the result. {s}`graph.transform(f)` is equivalent to {s}`f(graph)`
  *
  *      {s}`f: RaphtoryGraph => RaphtoryGraph`
  *      : function to apply
  *
  *  {s}`transform(algorithm: GraphAlgorithm): RaphtoryGraph`
  *    : Execute only the apply step of the algorithm on every view and returns a new {s}`RaphtoryGraph` with the
  *    result.
  *
  *      {s}`algorithm: GraphAlgorithm`
  *      : algorithm to apply
  *
  *  {s}`execute(f: RaphtoryGraph => Table): Table`
  *    : Apply f over itself and return the result. {s}`graph.execute(f)` is equivalent to {s}`f(graph)`
  *
  *      {s}`f: RaphtoryGraph => Table`
  *      : function to apply
  *
  *  {s}`execute(algorithm: GraphAlgorithm): Table`
  *    : Execute the algorithm on every view and returns a new {s}`RaphtoryGraph` with the result.
  *
  *      {s}`algorithm: GraphAlgorithm`
  *      : algorithm to apply
  *
  * ```{seealso}
  * [](com.raphtory.core.algorithm.GraphOperations)
  * ```
  */
trait RaphtoryGraph extends GraphOperations[RaphtoryGraph] {
  def transform(f: RaphtoryGraph => RaphtoryGraph): RaphtoryGraph = f(this)
  def transform(algorithm: GraphAlgorithm): RaphtoryGraph
  def execute(f: RaphtoryGraph => Table): Table                   = f(this)
  def execute(algorithm: GraphAlgorithm): Table
}
