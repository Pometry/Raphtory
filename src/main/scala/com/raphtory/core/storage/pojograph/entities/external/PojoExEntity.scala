package com.raphtory.core.storage.pojograph.entities.external

import com.raphtory.core.graph.visitor.EntityVisitor
import com.raphtory.core.graph.visitor.HistoricEvent
import com.raphtory.core.storage.pojograph.PojoGraphLens
import com.raphtory.core.storage.pojograph.entities.internal.PojoEntity

import scala.reflect.ClassTag

/** @DoNotDocument */
abstract class PojoExEntity(entity: PojoEntity, view: PojoGraphLens) extends EntityVisitor {
  def Type(): String = entity.getType

  def firstActivityAfter(time: Long): HistoricEvent = {
    //history().filter(k => k.time >= time).minBy(x => x.time)
    val res = history()._1.zipWithIndex.filter({case (k,c)  => k >= time}).minBy(x => x._1)
    HistoricEvent(res._1, history()._2(res._2))
  }

  def lastActivityBefore(time: Long): HistoricEvent = {
    val res = history()._1.zipWithIndex.filter({case (k,c)  => k <= time}).maxBy(x => x._1)
    HistoricEvent(res._1, history()._2(res._2))
  }

  def latestActivity(): HistoricEvent = HistoricEvent(history()._1(history()._1.length-1), history()._2(history()._2.length-1))

  def earliestActivity(): HistoricEvent = {
    val res = history()._1.zipWithIndex.minBy(x => x._1)
    HistoricEvent(res._1, history()._2(res._2))
  }

  def getPropertySet(): List[String] =
    entity.properties.filter(p => p._2.creation() <= view.timestamp).keys.toList

//  def getProperty[T](key: String): Option[T] = getPropertyAt[T](key, view.timestamp)
//
//  def getPropertyOrElse[T](key: String, otherwise: T): T =
//    getPropertyAt[T](key, view.timestamp) match {
//      case Some(v) => v
//      case None    => otherwise
//    }

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
    (entity.properties.get(key), view.window) match {
      case (Some(p), Some(w)) =>
        Some(
                p.valueHistory(
                        math.max(view.timestamp - w, after),
                        math.min(view.timestamp, before)
                ).toList
                  .map(x => (x._1, x._2.asInstanceOf[T]))
        )
      case (Some(p), None)    =>
        Some(
                p.valueHistory(after, before = math.min(view.timestamp, before))
                  .toList
                  .map(x => (x._1, x._2.asInstanceOf[T]))
        )
      case (None, _)          => None
    }

  def history(): (Array[Long], Array[Boolean]) = {
    val historyFilter = new Array[Long](0)
    val historyEvent = new Array[Boolean](0)
    view.window match {
      case Some(w) =>
        entity.historyTime.zipWithIndex.foreach { case (time, count) =>
          if (time <= view.timestamp && time >= view.timestamp - w) {
            historyFilter :+ time
            historyEvent :+ entity.historyValue(count)
          }
        }
        (historyFilter, historyEvent)
    //        entity.history.long2BooleanEntrySet().stream()
    //          .filter(e => e.getLongKey <= view.timestamp && e.getLongKey >= view.timestamp - w)
    //          .map(e => HistoricEvent(e.getLongKey, e.getBooleanValue))
    //          .toArray.toList.asInstanceOf[List[HistoricEvent]]
      case None    =>
        entity.historyTime.zipWithIndex.foreach { case (time, count) =>
          if (time <= view.timestamp) {
            historyFilter :+ time
            historyEvent :+ entity.historyValue(count)
          }
        }
        (historyFilter, historyEvent)
      //        entity.history.long2BooleanEntrySet().stream()
    //          .filter( e => e.getLongKey <= view.timestamp)
    //        .map(e => HistoricEvent(e.getLongKey, e.getBooleanValue))
    //        .toArray().toList.asInstanceOf[List[HistoricEvent]]
    }
  }

  def aliveAt(time: Long): Boolean = entity.aliveAt(time)

  def aliveAt(time: Long, window: Long): Boolean = entity.aliveAtWithWindow(time, window)

  def active(after: Long = 0, before: Long = Long.MaxValue): Boolean =
    // entity.history.keySet.longStream().allMatch(k => k > after && k <= before)
    !entity.historyTime.filter(k => k > after && k <= before).isEmpty
}
