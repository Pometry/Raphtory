package com.raphtory.core.graph.visitor

import scala.reflect.ClassTag

abstract class EntityVisitor {
  def Type()

  def firstActivityAfter(time: Long): HistoricEvent
  def lastActivityBefore(time: Long): HistoricEvent
  def latestActivity(): HistoricEvent
  def earliestActivity(): HistoricEvent

  def getPropertySet(): List[String]
  def getProperty[T](key: String): Option[T]
  def getPropertyOrElse[T](key: String, otherwise: T): T
  def getPropertyAt[T](key: String, time: Long): Option[T]

  def getPropertyValues[T](
      key: String,
      after: Long = Long.MaxValue,
      before: Long = Long.MaxValue
  ): Option[List[T]]

  def getPropertyHistory[T](key: String): Option[List[(Long, T)]]

  //functionality to access the history of the edge + helpers
  def history(): List[HistoricEvent]
  def active(after: Long = Long.MaxValue, before: Long = Long.MaxValue): Boolean
  def aliveAt(time: Long, window: Long = Long.MaxValue): Boolean

  lazy val numCreations: Long = history().count(f => f.event)
  lazy val numDeletions: Long = history().count(f => !f.event)
}
case class HistoricEvent(time: Long, event: Boolean)

object EdgeDirection extends Enumeration {
  type Direction = Value
  val Incoming, Outgoing, Both = Value
}