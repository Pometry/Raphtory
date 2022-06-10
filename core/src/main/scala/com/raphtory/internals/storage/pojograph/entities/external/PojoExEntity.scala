package com.raphtory.internals.storage.pojograph.entities.external

import com.raphtory.api.analysis.visitor.EntityVisitor
import com.raphtory.api.analysis.visitor.HistoricEvent
import com.raphtory.internals.storage.pojograph.PojoGraphLens
import com.raphtory.internals.storage.pojograph.entities.internal.PojoEntity
import com.raphtory.utils.OrderedBuffer.TupleByFirstOrdering

import scala.collection.Searching.Found
import scala.collection.Searching.InsertionPoint

abstract private[raphtory] class PojoExEntity(entity: PojoEntity, view: PojoGraphLens)
        extends EntityVisitor {
  def Type(): String = entity.getType

  def firstActivityAfter(time: Long, strict: Boolean = true): Option[HistoricEvent] =
    if (
            strict && time >= entity.latestPoint
    ) // >= because if it equals the latest point there is nothing that happens after it
      None
    else if (!strict && time > entity.latestPoint)
      None
    else
      entity.history.search((time, None)) match {
        case Found(i)          =>
          val activity = if (strict) entity.history(i + 1) else entity.history(i)
          Some(HistoricEvent(activity._1, activity._2))
        case InsertionPoint(i) =>
          val activity =
            entity.history(i) // is not plus one as this would be the value above `time`
          Some(HistoricEvent(activity._1, activity._2))
      }

  def lastActivityBefore(time: Long, strict: Boolean = true): Option[HistoricEvent] =
    if (
            strict && time <= entity.oldestPoint
    ) // <= because if its the oldest time there cannot be an event before it
      None
    else if (!strict && time < entity.oldestPoint)
      None
    else
      entity.history.search((time, None)) match {
        case Found(i)          =>
          val activity =
            if (strict)
              entity.history(i - 1)
            else entity.history(i)
          Some(HistoricEvent(activity._1, activity._2))
        case InsertionPoint(i) =>
          val activity = entity.history(i - 1)
          Some(HistoricEvent(activity._1, activity._2))
      }

  def latestActivity(): HistoricEvent = history().head

  def earliestActivity(): HistoricEvent = history().minBy(k => k.time)

  def getPropertySet(): List[String] =
    entity.properties.filter(p => p._2.creation() <= view.end).keys.toList

  def getPropertyAt[T](key: String, time: Long): Option[T] =
    entity.properties.get(key) match {
      case Some(p) =>
        p.valueAt(time) match {
          case Some(v) => Some(v.asInstanceOf[T])
          case None    => None
        }
      case None    => None
    }

//  //TODo ADD Before
//  def getPropertyValues[T](key: String, after: Long, before: Long): Option[List[T]] =
//    entity.properties.get(key) match {
//      case Some(p) => Some(p.valuesAfter(after).toList.map(_.asInstanceOf[T]))
//      case None    => None
//    }

  def getPropertyHistory[T](
      key: String,
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): Option[List[(Long, T)]] =
    entity.properties.get(key) map { p =>
      p.valueHistory(
              math.max(view.start, after),
              math.min(view.end, before)
      ).toList
        .map(x => (x._1, x._2.asInstanceOf[T]))
    }

  def history(): List[HistoricEvent] =
    entity.history.collect {
      case (time, state) if time >= view.start && time <= view.end =>
        HistoricEvent(time, state)
    }.toList

  def aliveAt(time: Long): Boolean = entity.aliveAt(time)

  // This function considers that the start of the window is exclusive, but PojoEntity.aliveBetween
  // considers both bounds as inclusive, we correct the start of the window by 1
  def aliveAt(time: Long, window: Long): Boolean = entity.aliveBetween(time - window + 1, time)

  def active(after: Long = 0, before: Long = Long.MaxValue): Boolean =
    entity.history.exists(k => k._1 > after && k._1 <= before)

}
