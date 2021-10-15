package com.raphtory.core.model.graph.visitor

import com.raphtory.core.implementations.objectgraph.entities.internal.RaphtoryEntity

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap
import scala.reflect.ClassTag

abstract class EntityVisitor {
  def Type()

  def firstActivityAfter(time: Long):HistoricEvent
  def latestActivity():HistoricEvent
  def earliestActivity():HistoricEvent

  def getPropertySet(): List[String]
  def getProperty[T: ClassTag](key: String): Option[T]
  def getPropertyOrElse[T: ClassTag](key: String,otherwise:T): T
  def getPropertyAt[T: ClassTag](key: String, time: Long): Option[T]
  def getPropertyValues[T: ClassTag](key: String, after: Long=Long.MaxValue, before: Long=Long.MaxValue): Option[List[T]]

  def getPropertyHistory[T: ClassTag](key: String): Option[List[(Long, T)]]

  //functionality to access the history of the edge + helpers
  def history():List[HistoricEvent]
  def active(after:Long=Long.MaxValue,before: Long=Long.MaxValue): Boolean
  def aliveAt(time: Long, window: Long=Long.MaxValue): Boolean
}
case class HistoricEvent(time:Long,event:Boolean)
