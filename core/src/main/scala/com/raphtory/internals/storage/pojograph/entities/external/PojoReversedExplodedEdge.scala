package com.raphtory.internals.storage.pojograph.entities.external

import com.raphtory.internals.storage.pojograph.PojoGraphLens
import com.raphtory.internals.storage.pojograph.entities.internal.PojoEdge

class PojoReversedExplodedEdge(
    objectEdge: PojoEdge,
    override val view: PojoGraphLens,
    override val ID: Long,
    override val start: Long,
    override val timestamp: Long
) extends PojoExplodedEdge(objectEdge, view, ID, start, timestamp) {}

private[raphtory] object PojoReversedExplodedEdge {

  def fromExplodedEdge(pojoExEdge: PojoExplodedEdge, timestamp: Long): PojoReversedExplodedEdge           =
    new PojoReversedExplodedEdge(
            pojoExEdge.edge,
            pojoExEdge.view,
            pojoExEdge.ID,
            pojoExEdge.start,
            timestamp
    )

  def fromReversedEdge(pojoExReversedEdge: PojoExReversedEdge, timestamp: Long): PojoReversedExplodedEdge =
    new PojoReversedExplodedEdge(
            pojoExReversedEdge.edge,
            pojoExReversedEdge.view,
            pojoExReversedEdge.ID,
            pojoExReversedEdge.start,
            timestamp
    )
}
