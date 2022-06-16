package com.raphtory.internals.storage.pojograph.entities.external

import com.raphtory.internals.storage.pojograph.PojoGraphLens
import com.raphtory.internals.storage.pojograph.entities.internal.PojoEdge

private[raphtory] class PojoExEdge(
    val edge: PojoEdge,
    override val ID: Long,
    val view: PojoGraphLens,
    start: Long,
    end: Long
) extends PojoExEntity(edge, view, start, end)
        with PojoExEdgeBase[Long] {

  def this(entity: PojoEdge, id: Long, view: PojoGraphLens) = {
    this(entity, id, view, view.start, view.end)
  }

  override type ExplodedEdge = PojoExplodedEdge

  def src: Long = edge.getSrcId

  def dst: Long = edge.getDstId

  override def explode(): List[ExplodedEdge] =
    history().collect { case event if event.event => PojoExplodedEdge.fromEdge(this, event.time) }

  def viewBetween(after: Long, before: Long) =
    new PojoExEdge(edge, ID, view, math.max(after, view.start), math.min(before, view.end))
}
