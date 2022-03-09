package com.raphtory.core.graph.visitor

import com.raphtory.core.graph.visitor.PropertyMergeStrategy.PropertyMerge

abstract class EntityVisitor {
  def Type()

  def firstActivityAfter(time: Long): HistoricEvent
  def lastActivityBefore(time: Long): HistoricEvent
  def latestActivity(): HistoricEvent
  def earliestActivity(): HistoricEvent

  def getPropertySet(): List[String]

  def getProperty[A, B](key: String, mergeStrategy: PropertyMerge[A, B]): Option[B] =
    getPropertyHistory[A](key) match {
      case Some(history) => Some(mergeStrategy(history))
      case None          => None
    }

  def getProperty[A](key: String): Option[A] = getProperty(key, PropertyMergeStrategy.latest)

  def getPropertyOrElse[A, B](key: String, otherwise: B, mergeStrategy: PropertyMerge[A, B]): B =
    getProperty[A, B](key, mergeStrategy) match {
      case Some(value) => value
      case None        => otherwise
    }

  def getPropertyOrElse[A](key: String, otherwise: A): A =
    getPropertyOrElse(key, otherwise, PropertyMergeStrategy.latest)

  def getPropertyAt[T](key: String, time: Long): Option[T]

  def getPropertyValues[T](
      key: String,
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): Option[List[T]] =
    getPropertyHistory[T](key) match {
      case Some(history) =>
        Some(history.collect({
          case (timestamp, value) if (after <= timestamp) && (timestamp <= before) => value
        }))
      case None          => None
    }

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
