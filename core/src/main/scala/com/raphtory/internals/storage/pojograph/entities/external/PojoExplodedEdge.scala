package com.raphtory.internals.storage.pojograph.entities.external

import com.raphtory.internals.storage.pojograph.PojoGraphLens
import com.raphtory.internals.storage.pojograph.entities.internal.PojoEdge

private[pojograph] class PojoExplodedEdge(
    val edge: PojoEdge,
    val view: PojoGraphLens,
    override val ID: Long,
    val start: Long,
    override val timestamp: Long
) extends PojoExEntity(edge, view, start, timestamp)
        with PojoExDirectedEdgeBase[PojoExplodedEdge, Long]
        with PojoExplodedEdgeBase[Long] {

  override type Eundir = PojoExplodedEdgeBase[Long]

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

private[pojograph] object PojoExplodedEdge {

  def fromEdge(pojoExEdge: PojoExEdge, timestamp: Long): PojoExplodedEdge =
    new PojoExplodedEdge(
            pojoExEdge.edge,
            pojoExEdge.view,
            pojoExEdge.ID,
            pojoExEdge.start,
            timestamp
    )
}

private[pojograph] class PojoExplodedInOutEdge(
    in: PojoExplodedEdge,
    out: PojoExplodedEdge,
    asInEdge: Boolean = false
) extends PojoExInOutEdgeBase[PojoExplodedInOutEdge, PojoExplodedEdge, Long](in, out, asInEdge)
        with PojoExplodedEdgeBase[Long] {

  override def timestamp: Long = in.timestamp
}

private[pojograph] class PojoReversedExplodedEdge(
    objectEdge: PojoEdge,
    override val view: PojoGraphLens,
    override val ID: Long,
    override val start: Long,
    override val timestamp: Long
) extends PojoExplodedEdge(objectEdge, view, ID, start, timestamp) {}

private[pojograph] object PojoReversedExplodedEdge {

  def fromExplodedEdge(pojoExEdge: PojoExplodedEdge, timestamp: Long): PojoReversedExplodedEdge =
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
