package com.raphtory.internals.storage.pojograph.entities.external

import com.raphtory.api.analysis.visitor.EntityVisitor
import com.raphtory.api.analysis.visitor.HistoricEvent
import com.raphtory.internals.storage.pojograph.PojoGraphLens
import com.raphtory.internals.storage.pojograph.entities.internal.PojoEntity
import com.raphtory.internals.storage.pojograph.OrderedBuffer.TupleByFirstOrdering

import scala.collection.IndexedSeqView
import scala.collection.Searching.Found
import scala.collection.Searching.InsertionPoint
import scala.collection.mutable.ArrayBuffer

abstract private[raphtory] class PojoExEntity(
    entity: PojoEntity,
    view: PojoGraphLens,
    start: Long,
    end: Long
) extends EntityVisitor {

  def this(entity: PojoEntity, view: PojoGraphLens) = {
    this(entity, view, view.start, view.end)
  }

  protected lazy val historyInView: IndexedSeqView[(Long, Boolean)] = {
    val history    = entity.history
    val startIndex = entity.history.search((start, None)).insertionPoint

    val endIndex = entity.history.search((end, None)) match {
      case Found(i)          => i + 1
      case InsertionPoint(i) => i
    }

    history.view.slice(startIndex, endIndex)
  }

  def Type(): String = entity.getType

  def firstActivityAfter(time: Long, strict: Boolean = true): Option[HistoricEvent] =
    if (strict && time >= historyInView.last._1)
      None // >= because if it equals the latest point there is nothing that happens after it
    else if (!strict && time > historyInView.last._1)
      None
    else {
      val index    = historyInView.search((time, None)) match {
        case Found(i)          => if (strict) i + 1 else i
        case InsertionPoint(i) => i // is not plus one as this would be the value above `time`
      }
      val activity = historyInView(index)
      Some(HistoricEvent(activity._1, activity._2))
    }

  def lastActivityBefore(time: Long, strict: Boolean = true): Option[HistoricEvent] =
    if (strict && time <= historyInView.head._1)
      None // <= because if its the oldest time there cannot be an event before it
    else if (!strict && time < historyInView.head._1)
      None
    else {
      val index    = historyInView.search((time, None)) match {
        case Found(i)          => if (strict) i - 1 else i
        case InsertionPoint(i) => i - 1

      }
      val activity = historyInView(index)
      Some(HistoricEvent(activity._1, activity._2))
    }

  def latestActivity(): HistoricEvent = history().last

  def earliestActivity(): HistoricEvent = history().head

  def getPropertySet(): List[String] =
    entity.properties.filter(p => p._2.creation() <= end).keys.toList

  def getPropertyAt[T](key: String, time: Long): Option[T] =
    if (timeIsInView(time))
      entity.properties.get(key) match {
        case Some(p) =>
          p.valueAt(time) match {
            case Some(v) => Some(v.asInstanceOf[T])
            case None    => None
          }
        case None    => None
      }
    else None

  //  //TODo ADD Before
//  def getPropertyValues[T](key: String, after: Long, before: Long): Option[List[T]] =
//    entity.properties.get(key) match {
//      case Some(p) => Some(p.valuesAfter(after).toList.map(_.asInstanceOf[T]))
//      case None    => None
//    }

  def getPropertyHistory[T](
      key: String,
      after: Long = start,
      before: Long = end
  ): Option[List[(Long, T)]] =
    entity.properties.get(key) map { p =>
      p.valueHistory(
              math.max(start, after),
              math.min(end, before)
      ).toList
        .map(x => (x._1, x._2.asInstanceOf[T]))
    }

  def history(): List[HistoricEvent] =
    historyInView.map { case (time, created) => HistoricEvent(time, created) }.toList

  def aliveAt(time: Long): Boolean   = timeIsInView(time) && entity.aliveAt(time)

  // This function considers that the start of the window is exclusive, but PojoEntity.aliveBetween
  // considers both bounds as inclusive, we correct the start of the window by 1
  def aliveAt(time: Long, window: Long): Boolean =
    time >= start &&
      time - window < end &&
      entity.aliveBetween(math.max(time - window + 1, start), math.min(time, end))

  def active(after: Long = Long.MinValue, before: Long = Long.MaxValue): Boolean =
    historyInView.exists(k => k._1 >= after && k._1 <= before)

  def timeIsInView(time: Long): Boolean = (time >= start) && (time <= end)
}
