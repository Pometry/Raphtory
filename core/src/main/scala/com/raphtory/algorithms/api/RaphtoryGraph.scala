package com.raphtory.algorithms.api

import com.raphtory.client.QuerySender
import com.raphtory.components.querymanager.Query

/**
  * {s}`RaphtoryGraph`
  *  : Core class for the analysis API
  *
  * A {s}`RaphtoryGraph` is an immutable collection of perspectives over a graph generated for Raphtory that support
  * all graph operations.
  * It implements the operations exposed by {s}`GraphOperations` returning a new {s}`RaphtoryGraph` for
  * those operations that have a graph as a result.
  * All the operations executed over a {s}`RaphtoryGraph` get executed individually over every perspective of the graph in the
  * collection. Graph states refer to the state of ever single perspective in the collection separately.
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
  *    : Execute only the apply step of the algorithm on every perspective and returns a new {s}`RaphtoryGraph` with the
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
  *    : Execute the algorithm on every perspective and returns a new {s}`RaphtoryGraph` with the result.
  *
  *      {s}`algorithm: GraphAlgorithm`
  *      : algorithm to apply
  *
  * ```{seealso}
  * [](com.raphtory.algorithms.api.GraphOperations)
  * ```
  */
private[raphtory] class RaphtoryGraph(query: Query, private val querySender: QuerySender)
        extends DefaultGraphOperations[RaphtoryGraph](query, querySender) {

  def transform(f: RaphtoryGraph => RaphtoryGraph): RaphtoryGraph = f(this)

  def transform(algorithm: GraphAlgorithm): RaphtoryGraph = {
    val graph            = new GenericGraphPerspective(query, querySender)
    val transformedGraph = algorithm.apply(graph)
    newGraph(transformedGraph.asInstanceOf[GenericGraphPerspective].query, querySender)
  }

  def execute(algorithm: GraphAlgorithm): Table =
    algorithm.run(new GenericGraphPerspective(query, querySender))

  def execute(f: RaphtoryGraph => Table): Table = f(this)

  override protected def newGraph(query: Query, querySender: QuerySender): RaphtoryGraph =
    new RaphtoryGraph(query, querySender)
}
