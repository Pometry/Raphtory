package com.raphtory.internals.storage.pojograph.entities.external.edge

import com.raphtory.api.analysis.visitor.IndexedValue
import com.raphtory.api.analysis.visitor.TimePoint
import com.raphtory.internals.storage.pojograph.PojoGraphLens
import com.raphtory.internals.storage.pojograph.entities.external.PojoExEntity
import com.raphtory.internals.storage.pojograph.entities.external.vertex._
import com.raphtory.internals.storage.pojograph.entities.internal.PojoEdge

private[pojograph] class PojoExplodedEdge(
    val edge: PojoEdge,
    override val view: PojoGraphLens,
    override val ID: Long,
    val timePoint: IndexedValue
) extends PojoExEntity(edge, view, timePoint, timePoint)
        with PojoExDirectedEdgeBase[PojoExplodedEdge, Long]
        with PojoExplodedEdgeBase[Long] {

  override type Eundir = PojoExplodedEdgeBase[Long]

  override def reversed: PojoReversedExplodedEdge =
    PojoReversedExplodedEdge.fromExplodedEdge(this)

  override def src: Long = edge.getSrcId

  override def dst: Long = edge.getDstId

  override def timestamp: Long = timePoint.time

  override def combineUndirected(other: PojoExplodedEdge, asInEdge: Boolean): PojoExplodedInOutEdge =
    if (isIncoming)
      new PojoExplodedInOutEdge(this, other, asInEdge)
    else
      new PojoExplodedInOutEdge(other, this, asInEdge)
}

private[pojograph] object PojoExplodedEdge {

  def fromEdge(pojoExEdge: PojoExEdge, timePoint: IndexedValue): PojoExplodedEdge =
    new PojoExplodedEdge(
            pojoExEdge.edge,
            pojoExEdge.view,
            pojoExEdge.ID,
            timePoint
    )
}

private[pojograph] class PojoExplodedInOutEdge(
    in: PojoExplodedEdge,
    out: PojoExplodedEdge,
    asInEdge: Boolean = false
) extends PojoExInOutEdgeBase[PojoExplodedInOutEdge, PojoExplodedEdge, Long](in, out, asInEdge)
        with PojoExplodedEdgeBase[Long] {

  override def timePoint: IndexedValue = in.timePoint
}

private[pojograph] class PojoReversedExplodedEdge(
    objectEdge: PojoEdge,
    override val view: PojoGraphLens,
    override val ID: Long,
    override val timePoint: IndexedValue
) extends PojoExplodedEdge(objectEdge, view, ID, timePoint) {
  override def src: Long = edge.getDstId

  override def dst: Long = edge.getSrcId
}

private[pojograph] object PojoReversedExplodedEdge {

  def fromExplodedEdge(pojoExEdge: PojoExplodedEdge): PojoReversedExplodedEdge = {
    val id = if (pojoExEdge.ID == pojoExEdge.src) pojoExEdge.dst else pojoExEdge.src
    new PojoReversedExplodedEdge(
            pojoExEdge.edge,
            pojoExEdge.view,
            id,
            pojoExEdge.timePoint
    )
  }

  def fromReversedEdge(pojoExReversedEdge: PojoExReversedEdge, timestamp: IndexedValue): PojoReversedExplodedEdge =
    new PojoReversedExplodedEdge(
            pojoExReversedEdge.edge,
            pojoExReversedEdge.view,
            pojoExReversedEdge.ID,
            timestamp
    )
}
