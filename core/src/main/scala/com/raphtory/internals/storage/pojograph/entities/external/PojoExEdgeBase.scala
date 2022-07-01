package com.raphtory.internals.storage.pojograph.entities.external

import com.raphtory.api.analysis.visitor.ConcreteEdge
import com.raphtory.api.analysis.visitor.ConcreteExplodedEdge
import com.raphtory.api.analysis.visitor.HistoricEvent
import com.raphtory.api.analysis.visitor.ReducedEdge
import com.raphtory.internals.components.querymanager.FilteredInEdgeMessage
import com.raphtory.internals.components.querymanager.FilteredOutEdgeMessage
import com.raphtory.internals.components.querymanager.VertexMessage
import com.raphtory.internals.storage.pojograph.PojoGraphLens

private[pojograph] trait PojoExEdgeBase[T] extends ConcreteEdge[T] {
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

private[pojograph] trait PojoExplodedEdgeBase[T] extends PojoExEdgeBase[T] with ConcreteExplodedEdge[T]

private[pojograph] trait PojoExDirectedEdgeBase[
    Edir <: PojoExDirectedEdgeBase[Edir, T],
    T
] extends PojoExEdgeBase[T] { this: Edir =>
  type Eundir >: Edir <: PojoExEdgeBase[T]
  def reversed: Edir

  def combineUndirected(other: Edir, asInEdge: Boolean): Eundir
}

private[pojograph] trait PojoExReducedEdgeBase extends PojoExEdgeBase[Long] with ReducedEdge {

  def viewBetween(after: Long, before: Long): PojoExReducedEdgeBase
}

abstract private[pojograph] class PojoExInOutEdgeBase[Eundir <: PojoExInOutEdgeBase[
        Eundir,
        Edir,
        T
], Edir <: PojoExDirectedEdgeBase[
        Edir,
        T
], T](in: Edir, out: Edir, asInEdge: Boolean)
        extends PojoExEdgeBase[T] { this: Eundir =>
  override def ID: IDType  = in.ID
  override def src: IDType = if (asInEdge) in.src else out.src
  override def dst: IDType = if (asInEdge) in.dst else out.dst

  override val view: PojoGraphLens = in.view

  val edges = List(in, out)

  override def remove(): Unit =
    edges.foreach(_.remove())

  override def send(data: Any): Unit = view.sendMessage(VertexMessage(view.superStep + 1, ID, data))

  override def Type(): String = edges.map(_.Type()).distinct.mkString("_")

  override def firstActivityAfter(time: Long, strict: Boolean): Option[HistoricEvent] =
    edges
      .flatMap(_.firstActivityAfter(time, strict)) // list of HistoricEvent that are defined
      .minByOption(_.time)                         // get most recent one if there are any

  override def lastActivityBefore(time: Long, strict: Boolean): Option[HistoricEvent] =
    edges
      .flatMap(_.lastActivityBefore(time, strict)) // list of HistoricEvent that are defined
      .maxByOption(_.time)                         // get latest one if there are any

  override def latestActivity(): HistoricEvent =
    edges
      .map(_.latestActivity())
      .maxBy(_.time) // get most recent one

  override def earliestActivity(): HistoricEvent =
    edges
      .map(_.earliestActivity())
      .minBy(_.time) // get most recent one

  override def getPropertySet(): List[String] = edges.flatMap(_.getPropertySet()).distinct

  override def getPropertyAt[T](key: String, time: Long): Option[T] =
    if (time < start || time > end)
      None
    else
      getPropertyHistory[T](key, start, time).map(_.last._2)

  override def getPropertyHistory[T](
      key: String,
      after: Long,
      before: Long
  ): Option[List[(Long, T)]] = {
    val histories = edges.map(_.getPropertyHistory[T](key, after, before))
    if (histories.forall(_.isEmpty))
      None
    else
      Some(histories.flatten.flatten.sortBy(_._1))
  }

  override def history(): List[HistoricEvent] =
    edges.flatMap(_.history()).sortBy(_.time).distinct

  override def active(after: Long, before: Long): Boolean =
    edges.exists(_.active(after, before))

  override def aliveAt(time: Long, window: Long): Boolean =
    edges.exists(_.aliveAt(time, window))

  override def start: Long = math.min(in.start, out.start)

  override def end: Long = math.max(in.end, out.end)
}
