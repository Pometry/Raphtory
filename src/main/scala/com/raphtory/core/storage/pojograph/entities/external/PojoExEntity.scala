package com.raphtory.core.storage.pojograph.entities.external

import com.raphtory.core.graph.visitor.EntityVisitor
import com.raphtory.core.graph.visitor.HistoricEvent
import com.raphtory.core.storage.pojograph.PojoGraphLens
import com.raphtory.core.storage.pojograph.entities.internal.PojoEntity

import scala.reflect.ClassTag

/** @DoNotDocument */
abstract class PojoExEntity(entity: PojoEntity, view: PojoGraphLens) extends EntityVisitor {
  def Type() = entity.getType

  def firstActivityAfter(time: Long): HistoricEvent =
    history().filter(k => k.time >= time).minBy(x => x.time)

  def lastActivityBefore(time: Long): HistoricEvent =
    history().filter(k => k.time <= time).maxBy(x => x.time)

  def latestActivity(): HistoricEvent = history().head

  def earliestActivity(): HistoricEvent = history().minBy(k => k.time)

  def getPropertySet(): List[String] =
    entity.properties.filter(p => p._2.creation() <= view.timestamp).keys.toList

  def getProperty[T: ClassTag](key: String): Option[T] = getPropertyAt[T](key, view.timestamp)

  def getPropertyOrElse[T: ClassTag](key: String, otherwise: T): T =
    getPropertyAt[T](key, view.timestamp) match {
      case Some(v) => v
      case None    => otherwise
    }

  def getPropertyAt[T: ClassTag](key: String, time: Long): Option[T] =
    entity.properties.get(key) match {
      case Some(p) =>
        p.valueAt(time) match {
          case Some(v) => Some(v.asInstanceOf[T])
          case None    => None
        }
      case None    => None
    }

  //TODo ADD Before
  def getPropertyValues[T: ClassTag](key: String, after: Long, before: Long): Option[List[T]] =
    entity.properties.get(key) match {
      case Some(p) => Some(p.valuesAfter(after).toList.map(_.asInstanceOf[T]))
      case None    => None
    }

  def getPropertyHistory[T: ClassTag](key: String): Option[List[(Long, T)]] =
    (entity.properties.get(key), view.window) match {
      case (Some(p), Some(w)) =>
        Some(
                p.values()
                  .filter(k => k._1 <= view.timestamp && k._1 >= view.timestamp - w)
                  .toList
                  .map(x => (x._1, x._2.asInstanceOf[T]))
        )
      case (Some(p), None)    =>
        Some(
                p.values()
                  .filter(k => k._1 <= view.timestamp)
                  .toList
                  .map(x => (x._1, x._2.asInstanceOf[T]))
        )
      case (None, _)          => None
    }

  def history(): List[HistoricEvent] =
    view.window match {
      case Some(w) =>
        entity.history.collect {
          case (time, state) if time <= view.timestamp && time >= view.timestamp - w =>
            HistoricEvent(time, state)
        }.toList
      case None    =>
        entity.history.collect {
          case (time, state) if time <= view.timestamp => HistoricEvent(time, state)
        }.toList
    }

  def aliveAt(time: Long): Boolean = entity.aliveAt(time)

  def aliveAt(time: Long, window: Long): Boolean = entity.aliveAtWithWindow(time, window)

  def active(after: Long = 0, before: Long = Long.MaxValue): Boolean =
    entity.history.exists(k => k._1 > after && k._1 <= before)

}
