package com.raphtory.internals.storage.pojograph.entities.external

import com.raphtory.api.analysis.visitor.ConcreteEdge
import com.raphtory.api.analysis.visitor.ReducedEdge
import com.raphtory.internals.components.querymanager.FilteredInEdgeMessage
import com.raphtory.internals.components.querymanager.FilteredOutEdgeMessage
import com.raphtory.internals.components.querymanager.VertexMessage
import com.raphtory.internals.storage.pojograph.PojoGraphLens

trait PojoExEdgeBase[T] extends ConcreteEdge[T] {
  def view: PojoGraphLens

  def start: Long

  def end: Long

  def send(data: Any): Unit =
    view.sendMessage(VertexMessage(view.superStep + 1, ID, data))

  override def remove(): Unit = {
    view.needsFiltering = true
    view.sendMessage(FilteredOutEdgeMessage(view.superStep + 1, src, dst))
    view.sendMessage(FilteredInEdgeMessage(view.superStep + 1, dst, src))
  }
}

trait PojoExDirectedEdgeBase[Edir <: PojoExDirectedEdgeBase[
        Edir,
        T
], T] extends PojoExEdgeBase[T] { this: Edir =>
  type Eundir <: PojoExEdgeBase[T]
  def reversed: Edir

  def combineUndirected(other: Edir, asInEdge: Boolean): Eundir
}

trait PojoExReducedEdgeBase extends PojoExEdgeBase[Long] with ReducedEdge {

  def viewBetween(after: Long, before: Long): PojoExReducedEdgeBase
}
