package com.raphtory.core.graph.visitor

import com.raphtory.core.graph.visitor.PropertyMergeStrategy.PropertyMerge

/**
  * {s}`EntityVisitor`
  *  : Common base class for [{s}`Edge`](com.raphtory.core.graph.visitor.Edge)
  *    and [{s}`Vertex`](com.raphtory.core.graph.visitor.Vertex)
  *
  * The {s}`EntityVisitor` class defines the interface for accessing properties (set when the graph is constructed)
  * and historic activity for vertices and edges.
  *
  * ## Methods
  *
  * {s}`Type(): String`
  *  : Return the type of the entity
  *
  * {s}`firstActivityAfter(time: Long): HistoricEvent`
  *  : Return next event (addition or deletion) after timestamp {s}`time` as an
  *    [{s}`HistoricEvent`](com.raphtory.core.graph.visitor.HistoricEvent)
  */
abstract class EntityVisitor {
  def Type(): String

  def firstActivityAfter(time: Long): HistoricEvent
  def lastActivityBefore(time: Long): HistoricEvent
  def latestActivity(): HistoricEvent
  def earliestActivity(): HistoricEvent

  def getPropertySet(): List[String]

  def getProperty[A, B](key: String, mergeStrategy: PropertyMerge[A, B]): Option[B] =
    getPropertyHistory[A](key) match {
      case Some(history) =>
        if (history.isEmpty)
          None
        else
          Some(mergeStrategy(history))
      case None          => None
    }

  def getProperty[A](key: String): Option[A] =
    getProperty(key, PropertyMergeStrategy.latest[A])

  def getPropertyOrElse[A, B](key: String, otherwise: B, mergeStrategy: PropertyMerge[A, B]): B =
    getProperty[A, B](key, mergeStrategy) match {
      case Some(value) => value
      case None        => otherwise
    }

  def getPropertyOrElse[A](key: String, otherwise: A): A =
    getPropertyOrElse(key, otherwise, PropertyMergeStrategy.latest[A])

  def getPropertyAt[T](key: String, time: Long): Option[T]

  def getPropertyValues[T](
      key: String,
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): Option[List[T]] =
    getPropertyHistory[T](key, after, before) match {
      case Some(history) =>
        Some(history.map({
          case (timestamp, value) => value
        }))
      case None          => None
    }

  def getPropertyHistory[T](
      key: String,
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): Option[List[(Long, T)]]

  //functionality to access the history of the edge or vertex + helpers
  def history(): List[HistoricEvent]
  def active(after: Long = Long.MinValue, before: Long = Long.MaxValue): Boolean
  def aliveAt(time: Long, window: Long = Long.MaxValue): Boolean

  lazy val numCreations: Long = history().count(f => f.event)
  lazy val numDeletions: Long = history().count(f => !f.event)
}

/**
  * # HistoricEvent
  *
  * {s}`HistoricEvent(time: Long, event: Boolean)`
  *  : Case class for encoding additions and deletions
  *
  *  ## Parameters
  *
  *  {s}`time: Long`
  *    : timestamp of event
  *
  *  {s}`event: Boolean`
  *    : {s}`true` if event is an addition or {s}`false` if event is a deletion
  */
case class HistoricEvent(time: Long, event: Boolean)

object EdgeDirection extends Enumeration {
  type Direction = Value
  val Incoming, Outgoing, Both = Value
}
