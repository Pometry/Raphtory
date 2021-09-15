package com.raphtory.core.model.implementations.objectgraph.entities.external

import com.raphtory.core.analysis.ObjectGraphLens
import com.raphtory.core.model.graph.visitor.{EntityVisitor, HistoricEvent}
import com.raphtory.core.model.implementations.objectgraph.entities.internal.RaphtoryEntity

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap

abstract class ObjectEntity(entity: RaphtoryEntity, view: ObjectGraphLens) extends EntityVisitor{
  def Type() = entity.getType

  def firstActivityAfter(time: Long) = history.filter(k => k.time >= time).minBy(x => x.time)
  def latestActivity()               = history.head
  def earliestActivity()             = history.minBy(k => k.time)

  def getPropertySet(): List[String] = {
      entity.properties.filter(p => p._2.creation() <= view.timestamp).map(f => (f._1)).toList
  }

  def getPropertyValue(key: String): Option[Any] =
    entity.properties.get(key) match {
      case Some(p) => Some(p.valueAt(view.timestamp))
      case None    => None
    }

  def getPropertyValueAt(key: String, time: Long): Option[Any] =
    entity.properties.get(key) match {
      case Some(p) => Some(p.valueAt(time))
      case None    => None
    }
  def getPropertyValues(key: String, after: Long, before: Long): Option[List[Any]] =
    entity.properties.get(key) match {
      case Some(p) => Some(p.valuesAfter(after).toList)
      case None    => None
    }

  def getPropertyHistory(key: String): Option[List[(Long, Any)]] =
    (entity.properties.get(key), view.window) match {
      case (Some(p), Some(w)) =>
        Some(p.values().filter(k => k._1 <= view.timestamp && k._1 >= view.timestamp - w).toList)
      case (Some(p), None) =>
        Some(p.values().filter(k => k._1 <= view.timestamp).toList)
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



  def active(after: Long=0, before: Long=Long.MaxValue): Boolean = entity.history.exists(k => k._1 > after && k._1 <= before)

}
