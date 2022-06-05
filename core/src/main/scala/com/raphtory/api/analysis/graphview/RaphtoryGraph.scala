package com.raphtory.api.analysis.graphview

import com.raphtory.internal.components.querymanager.Query
import com.raphtory.internal.management.client.QuerySender

/** Reduced GraphView with fixed timeline
  *
  * This [[GraphView]] is returned by the [[DottedGraph]] operations and does not support further timeline manipulation.
  *
  * @see [[GraphView]], [[MultilayerRaphtoryGraph]], [[DottedGraph]]
  */
class RaphtoryGraph private[api] (
    override private[api] val query: Query,
    override private[api] val querySender: QuerySender
) extends RaphtoryGraphBase[RaphtoryGraph]
        with ReducedGraphViewImplementation[RaphtoryGraph, MultilayerRaphtoryGraph] {}

/** Multilayer GraphView with fixed timeline */
class MultilayerRaphtoryGraph private[api] (
    override private[api] val query: Query,
    override private[api] val querySender: QuerySender
) extends RaphtoryGraphBase[MultilayerRaphtoryGraph]
        with MultilayerGraphViewImplementation[MultilayerRaphtoryGraph, RaphtoryGraph] {}

private[api] trait RaphtoryGraphBase[G <: RaphtoryGraphBase[G]]
        extends GraphBase[G, RaphtoryGraph, MultilayerRaphtoryGraph] {

  override private[api] def newRGraph(query: Query, querySender: QuerySender): RaphtoryGraph =
    new RaphtoryGraph(query, querySender)

  override private[api] def newMGraph(
      query: Query,
      querySender: QuerySender
  ): MultilayerRaphtoryGraph =
    new MultilayerRaphtoryGraph(query, querySender)
}
