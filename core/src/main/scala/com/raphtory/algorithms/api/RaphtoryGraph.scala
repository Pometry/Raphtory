package com.raphtory.algorithms.api

import com.raphtory.client.QuerySender
import com.raphtory.components.querymanager.Query
import com.raphtory.graph.visitor.Vertex

/** Core class for the analysis API.
  *
  * A RaphtoryGraph is an immutable collection of perspectives over a graph generated for Raphtory that support
  * all graph operations.
  * It implements the operations exposed by GraphOperations returning a new RaphtoryGraph for
  * those operations that have a graph as a result.
  * All the operations executed over a RaphtoryGraph get executed individually over every perspective of the graph in the
  * collection. Graph states refer to the state of ever single perspective in the collection separately.
  *
  * @see [[com.raphtory.algorithms.api.GraphOperations]]
  */
private[raphtory] class RaphtoryGraph(
    query: Query,
    private val querySender: QuerySender
) extends DefaultGraphOperations(query, querySender) {
  override type G  = RaphtoryGraph
  override type MG = RaphtoryGraph
  override type RG = RaphtoryGraph

  /** Apply f over itself and return the result. `graph.transform(f)` is equivalent to `f(graph)`
    * @param f function to apply
    * */
  def transform(f: RaphtoryGraph => RaphtoryGraph): RaphtoryGraph = f(this)

  /**  Execute only the apply step of the algorithm on every perspective and returns a new RaphtoryGraph with the result.
    *  @param algorithm algorithm to apply
    *  */
  def transform(algorithm: GraphAlgorithm): RaphtoryGraph = {
    val graph            = new GenericGraphPerspective(query, querySender)
    val transformedGraph = algorithm.apply(graph).clearMessages()
    newGraph(transformedGraph.asInstanceOf[GenericGraphPerspective].query, querySender)
  }

  /** Execute the algorithm on every perspective and returns a new `RaphtoryGraph` with the result.
    * @param algorithm to apply
    * */
  def execute(algorithm: GraphAlgorithm): Table =
    algorithm.run(
            new GenericGraphPerspective(
                    query.copy(name = algorithm.getClass.getSimpleName),
                    querySender
            )
    )

  /** Apply f over itself and return the result. `graph.execute(f)` is equivalent to `f(graph)`
    *  @param f function to apply
    */
  def execute(f: RaphtoryGraph => Table): Table = f(this)

  override protected def newGraph(
      query: Query,
      querySender: QuerySender
  ): RaphtoryGraph =
    new RaphtoryGraph(query, querySender)

  override protected def newRGraph(query: Query, querySender: QuerySender): RaphtoryGraph =
    newGraph(query, querySender)

  override protected def newMGraph(query: Query, querySender: QuerySender): RaphtoryGraph =
    newGraph(query, querySender)
}
