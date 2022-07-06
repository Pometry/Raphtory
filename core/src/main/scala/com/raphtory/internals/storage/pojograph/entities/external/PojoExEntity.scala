package com.raphtory.internals.storage.pojograph.entities.external

import com.raphtory.api.analysis.visitor.EntityVisitor
import com.raphtory.api.analysis.visitor.HistoricEvent
import com.raphtory.api.analysis.visitor.PropertyValue
import com.raphtory.internals.storage.pojograph.PojoGraphLens
import com.raphtory.internals.storage.pojograph.entities.internal.PojoEntity

import scala.collection.IndexedSeqView
import scala.collection.Searching.Found
import scala.collection.Searching.InsertionPoint

abstract private[pojograph] class PojoExEntity(
    entity: PojoEntity,
    view: PojoGraphLens,
    start: Long,
    end: Long
) extends EntityVisitor {

  def this(entity: PojoEntity, view: PojoGraphLens) = {
    this(entity, view, view.start, view.end)
  }

  protected lazy val historyInView: IndexedSeqView[HistoricEvent] = entity.viewHistoryBetween(start, end)

  def Type(): String = entity.getType

  def firstActivityAfter(time: Long, strict: Boolean = true): Option[HistoricEvent] =
    if (strict && time >= historyInView.last.time)
      None // >= because if it equals the latest point there is nothing that happens after it
    else if (!strict && time > historyInView.last.time)
      None
    else {
      val searchPoint = if (strict) HistoricEvent(time, Long.MaxValue) else HistoricEvent(time, Long.MinValue)
      val index       = historyInView.search(searchPoint) match {
        case Found(i)          => if (strict) i + 1 else i
        case InsertionPoint(i) => i // is not plus one as this would be the value above `time`
      }
      Some(historyInView(index))
    }

  def lastActivityBefore(time: Long, strict: Boolean = true): Option[HistoricEvent] =
    if (strict && time <= historyInView.head.time)
      None // <= because if its the oldest time there cannot be an event before it
    else if (!strict && time < historyInView.head.time)
      None
    else {
      val searchPoint = if (strict) HistoricEvent(time, Long.MinValue) else HistoricEvent(time, Long.MaxValue)
      val index       = historyInView.search(searchPoint) match {
        case Found(i)          => if (strict) i - 1 else i
        case InsertionPoint(i) => i - 1

      }
      Some(historyInView(index))
    }

  def latestActivity(): HistoricEvent = historyInView.last

  def earliestActivity(): HistoricEvent = historyInView.head

  def getPropertySet(): List[String] =
    entity.properties.filter(p => p._2.creation <= end).keys.toList

  //  //TODo ADD Before
//  def getPropertyValues[T](key: String, after: Long, before: Long): Option[List[T]] =
//    entity.properties.get(key) match {
//      case Some(p) => Some(p.valuesAfter(after).toList.map(_.asInstanceOf[T]))
//      case None    => None
//    }

  def getPropertyHistory[T](
      key: String,
      after: Long,
      before: Long
  ): Option[List[PropertyValue[T]]] =
    entity.properties.get(key) map { p =>
      p.valueHistory(
              math.max(start, after),
              math.min(end, before)
      ).toList
        .map(_.asInstanceOf[PropertyValue[T]])
    }

  def history(): List[HistoricEvent] =
    historyInView.toList

  def aliveAt(time: Long): Boolean = timeIsInView(time) && entity.aliveAt(time)

  // This function considers that the start of the window is exclusive, but PojoEntity.aliveBetween
  // considers both bounds as inclusive, we correct the start of the window by 1
  def aliveAt(time: Long, window: Long): Boolean =
    time >= start &&
      time - window < end &&
      entity.aliveBetween(math.max(time - window + 1, start), math.min(time, end))

  def active(after: Long = Long.MinValue, before: Long = Long.MaxValue): Boolean =
    historyInView.exists(k => k.time >= after && k.time <= before)

  def timeIsInView(time: Long): Boolean = (time >= start) && (time <= end)
}
