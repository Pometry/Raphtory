package com.raphtory.core.model.graph.visitor

import com.raphtory.core.model.implementations.objectgraph.ObjectGraphLens
import com.raphtory.core.model.implementations.objectgraph.entities.internal.RaphtoryEntity

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap

abstract class EntityVisitor {
  def Type()

  def firstActivityAfter(time: Long):HistoricEvent
  def latestActivity():HistoricEvent
  def earliestActivity():HistoricEvent

  def getPropertySet(): List[String]
  def getPropertyValue(key: String): Option[Any]
  def getPropertyValueAt(key: String, time: Long): Option[Any]
  def getPropertyValues(key: String, after: Long=Long.MaxValue, before: Long=Long.MaxValue): Option[List[Any]]

  def getPropertyHistory(key: String): Option[List[(Long, Any)]]

  //functionality to access the history of the edge + helpers
  def history():List[HistoricEvent]
  def active(after:Long=Long.MaxValue,before: Long=Long.MaxValue): Boolean
  def aliveAt(time: Long, window: Long=Long.MaxValue): Boolean
}
case class HistoricEvent(time:Long,event:Boolean)
