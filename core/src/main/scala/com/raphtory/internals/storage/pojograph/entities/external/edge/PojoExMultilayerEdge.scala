package com.raphtory.internals.storage.pojograph.entities.external.edge

import com.raphtory.api.analysis.visitor.EntityVisitor
import com.raphtory.api.analysis.visitor.HistoricEvent
import com.raphtory.api.analysis.visitor.PropertyValue
import com.raphtory.internals.storage.pojograph.PojoGraphLens

private[pojograph] class PojoExMultilayerEdge(
    override val timestamp: Long,
    override val ID: (Long, Long),
    override val src: (Long, Long),
    override val dst: (Long, Long),
    protected val edge: EntityVisitor,
    override val view: PojoGraphLens
) extends PojoExDirectedEdgeBase[PojoExMultilayerEdge, (Long, Long)]
        with PojoExplodedEdgeBase[(Long, Long)] {

  override def Type(): String = edge.Type()

  override def firstActivityAfter(time: Long, strict: Boolean): Option[HistoricEvent] =
    edge.firstActivityAfter(time, strict)

  override def lastActivityBefore(time: Long, strict: Boolean): Option[HistoricEvent] =
    edge.lastActivityBefore(time, strict)

  override def latestActivity(): HistoricEvent = edge.latestActivity()

  override def earliestActivity(): HistoricEvent = edge.earliestActivity()

  override def getPropertySet(): List[String] = edge.getPropertySet()

  override def getPropertyAt[T](key: String, time: Long): Option[T] = edge.getPropertyAt(key, time)

  override def getPropertyHistory[T](
      key: String,
      after: Long,
      before: Long = timestamp
  ): Option[Iterable[PropertyValue[T]]] = edge.getPropertyHistory[T](key, after, before)

  override def history(): List[HistoricEvent] = edge.history()

  override def active(after: Long, before: Long): Boolean = edge.active(after, before)

  override def aliveAt(time: Long, window: Long): Boolean = edge.aliveAt(time, window)

  override def reversed: PojoExMultilayerEdge =
    new PojoExMultilayerEdge(timestamp, ID, dst, src, edge, view)

  override type Eundir = PojoExplodedEdgeBase[(Long, Long)]

  override def combineUndirected(other: PojoExMultilayerEdge, asInEdge: Boolean): PojoExMultilayerInOutEdge =
    if (isIncoming)
      new PojoExMultilayerInOutEdge(this, other, asInEdge)
    else
      new PojoExMultilayerInOutEdge(other, this, asInEdge)

  override def start: Long = view.start

  override def end: Long = timestamp
}

private[pojograph] class PojoExMultilayerInOutEdge(
    in: PojoExMultilayerEdge,
    out: PojoExMultilayerEdge,
    asInEdge: Boolean
) extends PojoExInOutEdgeBase[PojoExMultilayerInOutEdge, PojoExMultilayerEdge, (Long, Long)](in, out, asInEdge)
        with PojoExplodedEdgeBase[(Long, Long)] {

  /** Timestamp for exploded entity */
  override def timestamp: Long = in.timestamp
}
