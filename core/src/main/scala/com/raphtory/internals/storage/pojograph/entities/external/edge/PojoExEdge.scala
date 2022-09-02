package com.raphtory.internals.storage.pojograph.entities.external.edge

import com.raphtory.api.analysis.visitor.HistoricEvent
import com.raphtory.api.analysis.visitor.IndexedValue
import com.raphtory.api.analysis.visitor.TimePoint
import com.raphtory.internals.storage.pojograph.PojoGraphLens
import com.raphtory.internals.storage.pojograph.entities.external.PojoExEntity
import com.raphtory.internals.storage.pojograph.entities.internal.PojoEdge
import com.raphtory.utils.OrderingFunctions._

private[pojograph] class PojoExEdge(
    val edge: PojoEdge,
    override val ID: Long,
    override val view: PojoGraphLens,
    override val start: IndexedValue,
    override val end: IndexedValue
) extends PojoExEntity(edge, view, start, end)
        with PojoExReducedEdgeImplementation[PojoExEdge]
        with PojoExDirectedEdgeBase[PojoExEdge, Long] {
  override type Eundir = PojoExReducedEdgeBase

  def this(entity: PojoEdge, id: Long, view: PojoGraphLens) = {
    this(entity, id, view, TimePoint.first(view.start), TimePoint.last(view.end))
  }

  override type ExplodedEdge = PojoExplodedEdge

  def src: Long = edge.getSrcId

  def dst: Long = edge.getDstId

  override def explode(): List[ExplodedEdge]                 =
    historyView.collect { case event if event.event => PojoExplodedEdge.fromEdge(this, event) }.toList

  def viewBetween(after: IndexedValue, before: IndexedValue) =
    new PojoExEdge(edge, ID, view, max(after, start), min(before, end))

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
        with PojoExReducedEdgeImplementation[PojoExInOutEdge] {

  override def viewBetween(after: IndexedValue, before: IndexedValue): PojoExInOutEdge =
    new PojoExInOutEdge(in.viewBetween(after, before), out.viewBetween(after, before), asInEdge)

  /** concrete type for exploded edge views of this edge which implements
    * [[ExplodedEdge]] with same `IDType`
    */
  override type ExplodedEdge = PojoExplodedEdgeBase[Long]

  /** Return an [[ExplodedEdge]] instance for each time the edge is
    * active in the current view.
    */
  override def explode(): List[ExplodedEdge] = {
    val inEdges         = in.explode().iterator
    val outEdges        = out.explode().iterator
    val transformInEdge =
      if (asInEdge) (edge: PojoExplodedEdge) => edge
      else (edge: PojoExplodedEdge) => edge.reversed

    val transformOutEdge =
      if (asInEdge) (edge: PojoExplodedEdge) => edge.reversed
      else (edge: PojoExplodedEdge) => edge

    val mergedIter: Iterator[PojoExplodedEdgeBase[Long]] = new Iterator[ExplodedEdge] {
      var inEdge: Option[PojoExplodedEdge]  = inEdges.nextOption()
      var outEdge: Option[PojoExplodedEdge] = outEdges.nextOption()

      override def hasNext: Boolean = inEdge.isDefined || outEdge.isDefined

      override def next(): ExplodedEdge =
        if (hasNext)
          inEdge match {
            case Some(ie) =>
              outEdge match {
                case Some(oe) =>
                  if (ie.timePoint < oe.timePoint) {
                    inEdge = inEdges.nextOption()
                    transformInEdge(ie)
                  }
                  else if (ie.timePoint > oe.timePoint) {
                    outEdge = outEdges.nextOption()
                    transformOutEdge(oe)
                  }
                  else {
                    inEdge = inEdges.nextOption()
                    outEdge = outEdges.nextOption()
                    new PojoExplodedInOutEdge(ie, oe, asInEdge)
                  }
                case None     =>
                  inEdge = inEdges.nextOption()
                  transformInEdge(ie)
              }
            case None     =>
              val oe = outEdge.get
              outEdge = outEdges.nextOption()
              transformOutEdge(oe)
          }
        else
          throw new NoSuchElementException
    }
    mergedIter.toList
  }
}

private[pojograph] class PojoExReversedEdge(
    override val edge: PojoEdge,
    override val ID: Long,
    override val view: PojoGraphLens,
    start: IndexedValue,
    end: IndexedValue
) extends PojoExEdge(edge, ID, view, start, end) {

  def this(entity: PojoEdge, id: Long, view: PojoGraphLens) = {
    this(entity, id, view, TimePoint.first(view.start), TimePoint.last(view.end))
  }

  override def src: Long = edge.getDstId

  override def dst: Long = edge.getSrcId

  override def explode(): List[ExplodedEdge]                          =
    historyView.collect { case event if event.event => PojoReversedExplodedEdge.fromReversedEdge(this, event) }.toList

  override def viewBetween(after: IndexedValue, before: IndexedValue) =
    new PojoExReversedEdge(edge, ID, view, max(after, start), min(before, end))

}

private[pojograph] object PojoExReversedEdge {

  def fromEdge(pojoExEdge: PojoExEdge): PojoExReversedEdge =
    fromEdge(pojoExEdge, pojoExEdge.start, pojoExEdge.end)

  def fromEdge(pojoExEdge: PojoExEdge, start: IndexedValue, end: IndexedValue): PojoExReversedEdge = {
    val id = if (pojoExEdge.ID == pojoExEdge.src) pojoExEdge.dst else pojoExEdge.src
    new PojoExReversedEdge(
            pojoExEdge.edge,
            id,
            pojoExEdge.view,
            max(pojoExEdge.start, start),
            min(pojoExEdge.end, end)
    )
  }
}
