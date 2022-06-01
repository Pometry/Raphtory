package com.raphtory.algorithms.api

import com.raphtory.client.QuerySender
import com.raphtory.components.querymanager.Query
import com.raphtory.graph.visitor.ExplodedVertex
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
  * @see [[com.raphtory.algorithms.api.GraphPerspective]]
  */
private[raphtory] class RaphtoryGraph(
    override private[api] val query: Query,
    override private[api] val querySender: QuerySender
) extends RaphtoryGraphBase[RaphtoryGraph]
        with ReducedGraphPerspectiveImplementation[RaphtoryGraph, MultilayerRaphtoryGraph] {}

class MultilayerRaphtoryGraph(
    override private[api] val query: Query,
    override private[api] val querySender: QuerySender
) extends RaphtoryGraphBase[MultilayerRaphtoryGraph]
        with MultilayerGraphPerspectiveImplementation[MultilayerRaphtoryGraph, RaphtoryGraph] {}

trait RaphtoryGraphBase[G <: RaphtoryGraphBase[G]]
        extends GraphBase[G, RaphtoryGraph, MultilayerRaphtoryGraph] {

  override protected def newRGraph(query: Query, querySender: QuerySender): RaphtoryGraph =
    new RaphtoryGraph(query, querySender)

  override protected def newMGraph(
      query: Query,
      querySender: QuerySender
  ): MultilayerRaphtoryGraph =
    new MultilayerRaphtoryGraph(query, querySender)
}
