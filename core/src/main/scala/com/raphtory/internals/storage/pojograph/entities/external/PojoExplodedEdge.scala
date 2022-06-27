package com.raphtory.internals.storage.pojograph.entities.external

import com.raphtory.api.analysis.visitor.ConcreteExplodedEdge
import com.raphtory.internals.storage.pojograph.PojoGraphLens
import com.raphtory.internals.storage.pojograph.entities.internal.PojoEdge

private[raphtory] class PojoExplodedEdge(
    val edge: PojoEdge,
    val view: PojoGraphLens,
    override val ID: Long,
    val start: Long,
    override val timestamp: Long
) extends PojoExEntity(edge, view, start, timestamp)
        with PojoExDirectedEdgeBase[PojoExplodedEdge, Long]
        with ConcreteExplodedEdge[Long] {

  override type Eundir = PojoExplodedInOutEdge

  override def reversed: PojoReversedExplodedEdge =
    PojoReversedExplodedEdge.fromExplodedEdge(this, timestamp)

  override def src: Long = edge.getSrcId

  override def dst: Long = edge.getDstId

  override def end: Long = timestamp

  override def combineUndirected(other: PojoExplodedEdge, asInEdge: Boolean): PojoExplodedInOutEdge =
    if (isIncoming)
      new PojoExplodedInOutEdge(this, other, asInEdge)
    else
      new PojoExplodedInOutEdge(other, this, asInEdge)
}

private[raphtory] object PojoExplodedEdge {

  def fromEdge(pojoExEdge: PojoExEdge, timestamp: Long): PojoExplodedEdge =
    new PojoExplodedEdge(
            pojoExEdge.edge,
            pojoExEdge.view,
            pojoExEdge.ID,
            pojoExEdge.start,
            timestamp
    )
}
