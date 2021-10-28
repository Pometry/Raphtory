package com.raphtory.core.implementations.pojograph.entities.external

import com.raphtory.core.implementations.pojograph.PojoGraphLens
import com.raphtory.core.implementations.pojograph.entities.internal.RaphtoryEntity
import com.raphtory.core.model.graph.visitor.{EntityVisitor, HistoricEvent}

import scala.collection.mutable
import scala.reflect.ClassTag

abstract class PojoEntity(entity: RaphtoryEntity, view: PojoGraphLens) extends EntityVisitor{
  def Type() = entity.getType

  def firstActivityAfter(time: Long) = history.filter(k => k.time >= time).minBy(x => x.time)
  def latestActivity()               = history.head
  def earliestActivity()             = history.minBy(k => k.time)

  def getPropertySet(): List[String] = {
      entity.properties.filter(p => p._2.creation() <= view.timestamp).map(f => (f._1)).toList
  }

  def getProperty[T: ClassTag](key: String): Option[T] =
    entity.properties.get(key) match {
      case Some(p) => Some(p.valueAt(view.timestamp).asInstanceOf[T])
      case None    => None
    }

  def getPropertyOrElse[T: ClassTag](key: String,otherwise:T): T = {
    entity.properties.get(key) match {
      case Some(p) => (p.valueAt(view.timestamp).asInstanceOf[T])
      case None    => otherwise
    }
  }


  def getPropertyAt[T: ClassTag](key: String, time: Long): Option[T] =
    entity.properties.get(key) match {
      case Some(p) => Some(p.valueAt(time).asInstanceOf[T])
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
        Some(p.values().filter(k => k._1 <= view.timestamp && k._1 >= view.timestamp - w).toList.map(x=> (x._1,x._2.asInstanceOf[T])))
      case (Some(p), None) =>
        Some(p.values().filter(k => k._1 <= view.timestamp).toList.map(x=> (x._1,x._2.asInstanceOf[T])))
      case (None, _) => None
    }

  def history():List[HistoricEvent] =
    view.window match {
      case Some(w) => entity.history.collect{
        case (time, state) if time <= view.timestamp && time >= view.timestamp - w => HistoricEvent(time,state)
      }.toList
      case None    => entity.history.collect{
        case (time, state) if time <= view.timestamp => HistoricEvent(time, state)
      }.toList
    }

  def aliveAt(time: Long): Boolean = entity.aliveAt(time)

  def aliveAt(time: Long, window: Long): Boolean = entity.aliveAtWithWindow(time, window)

  def active(after: Long=0, before: Long=Long.MaxValue): Boolean = entity.history.exists(k => k._1 > after && k._1 <= before)

}
