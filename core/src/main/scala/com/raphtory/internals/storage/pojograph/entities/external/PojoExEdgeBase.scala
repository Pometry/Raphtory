package com.raphtory.internals.storage.pojograph.entities.external

import com.raphtory.api.analysis.visitor.ConcreteEdge
import com.raphtory.internals.components.querymanager.FilteredInEdgeMessage
import com.raphtory.internals.components.querymanager.FilteredOutEdgeMessage
import com.raphtory.internals.components.querymanager.VertexMessage
import com.raphtory.internals.storage.pojograph.PojoGraphLens

trait PojoExEdgeBase[IDType] extends ConcreteEdge[IDType] {
  val view: PojoGraphLens

  def send(data: Any): Unit =
    view.sendMessage(VertexMessage(view.superStep + 1, ID, data))

  override def remove(): Unit = {
    view.needsFiltering = true
    view.sendMessage(FilteredOutEdgeMessage(view.superStep + 1, src, dst))
    view.sendMessage(FilteredInEdgeMessage(view.superStep + 1, dst, src))
  }
}
