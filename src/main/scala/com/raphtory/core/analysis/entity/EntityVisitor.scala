package com.raphtory.core.analysis.entity

import com.raphtory.core.analysis.GraphLens
import com.raphtory.core.model.implementations.objectgraph.entities.RaphtoryEntity

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap

abstract class EntityVisitor(entity: RaphtoryEntity, view: GraphLens) {
  def Type() = entity.getType

  def firstActivityAfter(time: Long) = getHistory.filter(k => k._1 >= time).minBy(x => x._1)._1
  def latestActivity()               = getHistory.head
  def earliestActivity()             = getHistory.minBy(k => k._1)

  def getPropertySet(): ParTrieMap[String, Any] = {
    val x =
      entity.properties.filter(p => p._2.creation() <= view.timestamp).map(f => (f._1, f._2.valueAt(view.timestamp)))
    x
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
  def getPropertyValuesAfter(key: String, time: Long): Option[Array[Any]] =
    entity.properties.get(key) match {
      case Some(p) => Some(p.valuesAfter(time))
      case None    => None
    }

  def getPropertyHistory(key: String): Option[Array[(Long, Any)]] =
    (entity.properties.get(key), view.window) match {
      case (Some(p), Some(w)) =>
        Some(p.values().filter(k => k._1 <= view.timestamp && k._1 >= view.timestamp - w))
      case (Some(p), None) =>
        Some(p.values().filter(k => k._1 <= view.timestamp))
      case (None, _) => None
    }

  def getHistory(): mutable.TreeMap[Long, Boolean] =
    view.window match {
      case Some(w) => entity.history.filter(k => k._1 <= view.timestamp && k._1 >= view.timestamp - w)
      case None    => entity.history.filter(k => k._1 <= view.timestamp)
    }
}
