package com.raphtory.internals.storage.pojograph.entities.external

import com.raphtory.internals.storage.pojograph.PojoGraphLens
import com.raphtory.internals.storage.pojograph.entities.internal.PojoEdge

class PojoExReversedEdge(
    override val edge: PojoEdge,
    override val ID: Long,
    override val view: PojoGraphLens,
    start: Long,
    end: Long
) extends PojoExEdge(edge, ID, view, start, end) {

  def this(entity: PojoEdge, id: Long, view: PojoGraphLens) = {
    this(entity, id, view, view.start, view.end)
  }

  override def src: Long = edge.getDstId

  override def dst: Long = edge.getSrcId

  override def explode(): List[ExplodedEdge]          =
    history().collect { case event if event.event => PojoReversedExplodedEdge.fromReversedEdge(this, event.time) }

  override def viewBetween(after: Long, before: Long) =
    new PojoExReversedEdge(edge, ID, view, math.max(after, start), math.min(before, end))

}

object PojoExReversedEdge {

  def fromEdge(pojoExEdge: PojoExEdge): PojoExReversedEdge =
    fromEdge(pojoExEdge, pojoExEdge.start, pojoExEdge.end)

  def fromEdge(pojoExEdge: PojoExEdge, start: Long, end: Long): PojoExReversedEdge = {
    val id = if (pojoExEdge.ID == pojoExEdge.src) pojoExEdge.dst else pojoExEdge.src
    new PojoExReversedEdge(
            pojoExEdge.edge,
            id,
            pojoExEdge.view,
            math.max(pojoExEdge.start, start),
            math.min(pojoExEdge.end, end)
    )
  }
}
