package com.raphtory.internals.storage.pojograph.entities.external

import com.raphtory.api.analysis.visitor.HistoricEvent
import com.raphtory.internals.components.querymanager.VertexMessage
import com.raphtory.internals.storage.pojograph.PojoGraphLens

class PojoExInOutEdge(
    in: PojoExEdge,
    out: PojoExEdge,
    override val ID: Long,
    override val src: Long,
    override val view: PojoGraphLens,
    start: Long,
    end: Long
) extends PojoExEdgeBase[Long] {
  val edges = List(in, out)
  override type ExplodedEdge = PojoExplodedEdge

  override def dst: IDType = ID

  override def explode(): List[ExplodedEdge] =
    edges.flatMap(_.explode())

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
      after: Long = start,
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

  def viewBetween(after: Long, before: Long): PojoExInOutEdge =
    new PojoExInOutEdge(
            in.viewBetween(after, before),
            out.viewBetween(after, before),
            ID,
            src,
            view,
            start,
            end
    )
}
