package com.raphtory.internals.storage.pojograph.entities.external

import com.raphtory.internals.storage.pojograph.PojoGraphLens
import com.raphtory.internals.storage.pojograph.entities.internal.PojoEdge

private[pojograph] class PojoExEdge(
    val edge: PojoEdge,
    override val ID: Long,
    val view: PojoGraphLens,
    val start: Long,
    val end: Long
) extends PojoExEntity(edge, view, start, end)
        with PojoExReducedEdgeBase
        with PojoExDirectedEdgeBase[PojoExEdge, Long] {
  override type Eundir = PojoExReducedEdgeBase

  def this(entity: PojoEdge, id: Long, view: PojoGraphLens) = {
    this(entity, id, view, view.start, view.end)
  }

  override type ExplodedEdge = PojoExplodedEdge

  def src: Long = edge.getSrcId

  def dst: Long = edge.getDstId

  override def explode(): List[ExplodedEdge] =
    history().collect { case event if event.event => PojoExplodedEdge.fromEdge(this, event.time) }

  def viewBetween(after: Long, before: Long) =
    new PojoExEdge(edge, ID, view, math.max(after, start), math.min(before, end))

  override def reversed: PojoExReversedEdge =
    PojoExReversedEdge.fromEdge(this)

  override def combineUndirected(other: PojoExEdge, asInEdge: Boolean): PojoExInOutEdge =
    if (isIncoming)
      new PojoExInOutEdge(this, other, asInEdge)
    else
      new PojoExInOutEdge(other, this, asInEdge)
}

private[pojograph] class PojoExInOutEdge(
    in: PojoExEdge,
    out: PojoExEdge,
    asInEdge: Boolean = false
) extends PojoExInOutEdgeBase[PojoExInOutEdge, PojoExEdge, Long](in, out, asInEdge)
        with PojoExReducedEdgeBase {

  override def viewBetween(after: Long, before: Long): PojoExInOutEdge =
    new PojoExInOutEdge(in.viewBetween(after, before), out.viewBetween(after, before), asInEdge)

  /** concrete type for exploded edge views of this edge which implements
    * [[ExplodedEdge]] with same `IDType`
    */
  override type ExplodedEdge = PojoExplodedInOutEdge

  /** Return an [[ExplodedEdge]] instance for each time the edge is
    * active in the current view.
    */
  override def explode(): List[ExplodedEdge] =
    in.explode().zip(out.explode()).map { case (e1, e2) => new PojoExplodedInOutEdge(e1, e2, asInEdge) }
}

private[pojograph] class PojoExReversedEdge(
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

private[pojograph] object PojoExReversedEdge {

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
