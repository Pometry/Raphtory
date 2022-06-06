package com.raphtory.api.analysis.visitor

import PropertyMergeStrategy.PropertyMerge
import io.sqooba.oss.timeseries.TimeSeries
import io.sqooba.oss.timeseries.immutable.EmptyTimeSeries
import io.sqooba.oss.timeseries.immutable.TSEntry

/** Common base class for [`Edge`](com.raphtory.graph.visitor.Edge) and [`Vertex`](com.raphtory.graph.visitor.Vertex)
  *
  * The `EntityVisitor` class defines the interface for accessing properties (set when the graph is constructed)
  * and historic activity for vertices and edges.
  *
  * ## Attributes
  *
  * `numCreations: Long`
  *  : number of addition events for the entity in the current view
  *
  * `numDeletions: Long`
  *  : number of deletion events for the entity in the current view
  *
  * ## Methods
  *
  * `Type(): String`
  *  : Return the type of the entity
  *
  * ### Activity
  *
  * `history(): List[HistoricEvent]`
  *  : Return list of all events (additions or deletions) in the current view. Each event is
  *    returned as an [`HistoricEvent`](com.raphtory.graph.visitor.HistoricEvent) which
  *    encodes whether the event is an addition or deletion and the time of the event.
  *
  * `active(after: Long = Long.MinValue, before: Long = Long.MaxValue): Boolean`
  *  : Return `true` if any event (addition or deletion) occurred during the time window starting at
  *    `after` and ending at `before` and `false` otherwise.
  *
  * `aliveAt(time: Long, window: Long = Long.MaxValue): Boolean`
  *  : Check if the entity is currently alive (i.e, the last event was an addition) at time `time`.
  *    If a value for `window` is given, the entity is considered as having been deleted if more
  *    than `window` time has passed since the last addition event.
  *
  * `firstActivityAfter(time: Long): Option[HistoricEvent]`
  *  : Return next event (addition or deletion) after timestamp `time` as an
  *    [`HistoricEvent`](com.raphtory.graph.visitor.HistoricEvent). This is wrapped in an option as
  *    it is possible that no activity occurred after the given time. An optional `strict` Boolean argument is also
  *     available which allows events exactly at the given time to be returned if set to `false`.
  *
  * `lastActivityBefore(time: Long): Option[HistoricEvent]`
  *  : Return the last event (addition or deletion) before timestamp `time` as an
  *    [`HistoricEvent`](com.raphtory.graph.visitor.HistoricEvent).  The result is wrapped in an option as it is
  *     possible that no activity occurred before the given time. An optional `strict` Boolean argument is also
  *     available which allows events exactly at the given time to be returned if set to `false`.
  *
  * `latestActivity(): HistoricEvent`
  *  : Return the most recent event (addition or deletion) in the current view as an
  *    [`HistoricEvent`](com.raphtory.graph.visitor.HistoricEvent)
  *
  * `earliestActivity(): HistoricEvent`
  *  : Return the first event (addition or deltion) in the current view as an
  *    [`HistoricEvent`](com.raphtory.graph.visitor.HistoricEvent)
  *
  * ### Property access
  *
  * `getPropertySet(): List[String]`
  *  : Return list of keys for available properties for the entity
  *
  * `getPropertyHistory[T](key: String, after: Long = Long.MinValue, before: Long=Long.MaxValue): Option[List[(Long, T)]]`
  *  : Return history of values for property `key`. Returns `None` if no property with name `key` exists.
  *    Otherwise returns a list of `(timestamp, value)` tuples (which may be empty).
  *
  *    `key: String`
  *      : name of property
  *
  *    `after: Long` (optional)
  *      : Only consider addition events in the current view that happened after time `after`
  *
  *    `before: Long` (optional)
  *      : Only consider addition events in the current view that happened before time `before`
  *
  *    The exact behaviour depends on the type of the property:
  *
  *    In case of a
  *    [`ImmutableProperty`](com.raphtory.components.graphbuilder.Properties), the history
  *    depends only on the creation time of the property. If the property was created within the current view, the
  *    history contains a single tuple with the value of the property and the timestamp given by the creation time
  *    of the property. If the property creation time is before the start of the current view, the history contains
  *    a single tuple with the value of the property and the timestamp given by the start time of the current view.
  *    If the property creation time is after the end of the current view, the history is empty.
  *
  *    For the [other property types](com.raphtory.components.graphbuilder.Properties), the history contains
  *    the timestamps and values for all addition events within the current view and is empty if the property exists
  *    but no addition events occurred in the current view.
  *
  *    ```{note}
  *    More property types with different value semantics are likely going to be added in the future.
  *    ```
  *
  * `getTimeSeriesPropertyHistory[T](key: String, after: Long = Long.MinValue, before: Long = Long.MaxValue): Option[TimeSeries[T]]`
  *  :  Return values for property `key`. Returns `None` if no property with name `key` exists.
  *     Otherwise returns a TimeSeries (from this library: https://github.com/Sqooba/scala-timeseries-lib) which may be empty.
  *     This function utilises the getPropertyHistory function and maps over the list of tuples, retrieving the timestamps to work out the individual TimeSeries entries.
  *
  *     `key: String`
  *       : name of property
  *
  *    `after: Long` (optional)
  *      : Only consider addition events in the current view that happened after time `after`
  *
  *    `before: Long` (optional)
  *      : Only consider addition events in the current view that happened before time `before`
  *
  * `getPropertyValues[T](key: String, after: Long = Long.MinValue, before: Long = Long.MaxValue): Option[List[T]]`
  *  :  Return values for property `key`. Returns `None` if no property with name `key` exists.
  *    Otherwise returns a list of values (which may be empty).
  *
  *    `key: String`
  *      : name of property
  *
  *    `after: Long` (optional)
  *      : Only consider addition events in the current view that happened after time `after`
  *
  *    `before: Long` (optional)
  *      : Only consider addition events in the current view that happened before time `before`
  *
  *    This method acts in the same way as `getPropertyHistory` with the difference that only property values
  *    are returned.
  *
  * `getProperty[A](key: String): Option[A]`
  *  : Get most recent value for property `key` in the current view.
  *    Returns `None` if the property does not exist or its history in the current view is empty.
  *
  *    `A`
  *      : Value type of the property
  *
  *    `key`
  *      : name of property
  *
  * `getProperty[A, B](key: String, mergeStrategy: List[(Long, A)] => B): Option[B]`
  *  : Apply a merge strategy to compute the value for property `key` based on its history in the current view.
  *    This function returns `None` if the property does not exist or its history in the current view is empty and
  *    the computed property value otherwise.
  *
  *    `A`
  *      : Value type of property
  *
  *    `B`
  *      : Return type of merge strategy
  *
  *    `key: String`
  *     : name of property
  *
  *    `mergeStrategy: List[(Long, A)] => B`
  *      : function to apply to the property history to compute the property value
  *
  *    The `mergeStrategy` can be an arbitrary function to apply to the property history. However, predefined
  *    merge strategies for common use cases are provided in
  *    [`PropertyMergeStrategy`](com.raphtory.graph.visitor.PropertyMergeStrategy). To e.g., compute the
  *    average value of a property use
  *    ```scala
  *    import com.raphtory.graph.visitor.PropertyMergeStrategy
  *
  *    val entity: EntityVisitor
  *    val average: Double = entity.getProperty[T, Double](key: String, PropertyMergeStrategy.average)
  *    ```
  *
  * `getPropertyOrElse[A](key: String, otherwise: A): A`
  *  : Return most recent value of property if it exists or a default value otherwise.
  *
  *    `A`
  *      : value type of property
  *
  *    `key: String`
  *      : property name
  *
  *    `otherwise: A`
  *     : default value to use if property value does not exist
  *
  * `getPropertyOrElse[A, B](key: String, otherwise: B, mergeStrategy: List[(Long, A)] => B): B`
  *  : Apply a merge strategy to compute the property value if the property exists and its history in the current view
  *    is not empty or a default value otherwise
  *
  *    `A`
  *      : value type of property
  *
  *    `B`
  *      : return type of merge strategy
  *
  *    `key: String`
  *      : property name
  *
  *    `otherwise: B`
  *      : default value to use if property value does not exist
  *
  *    `mergeStrategy: List[(Long, A)] => B`
  *      : function to apply to the property history to compute the property value
  *
  * `getPropertyAt[T](key: String, time: Long): Option[T]`
  *  : Get the most recent value of property `key` before time `time`
  *
  *    `T`
  *      : value type of property
  *
  *    `key: String`
  *      : property name
  *
  *    `time: Long`
  *      : time stamp for property lookup
  *
  * ```{seealso}
  * [](com.raphtory.graph.visitor.Edge),
  * [](com.raphtory.graph.visitor.Vertex),
  * [](com.raphtory.graph.visitor.PropertyMergeStrategy),
  * [](com.raphtory.graph.visitor.HistoricEvent)
  * ```
  */
abstract class EntityVisitor {

  def Type(): String

  def firstActivityAfter(time: Long, strict: Boolean = true): Option[HistoricEvent]
  def lastActivityBefore(time: Long, strict: Boolean = true): Option[HistoricEvent]
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

  def getTimeSeriesPropertyHistory[T](
      key: String,
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): Option[TimeSeries[T]] =
    getPropertyHistory[T](key, after, before).map { timestampList =>
      if (timestampList.nonEmpty)
        TimeSeries.ofOrderedEntriesUnsafe {
          timestampList.iterator
            .sliding(2)
            .withPartial(false)
            .map {
              case List((timestamp1, value1), (timestamp2, value2)) =>
                TSEntry[T](timestamp1, value1, timestamp2 - timestamp1)
            }
            .++(Seq(TSEntry[T](timestampList.last._1, timestampList.last._2, before)))
            .toSeq
        }
      else EmptyTimeSeries
    }

  //functionality to access the history of the edge or vertex + helpers
  def history(): List[HistoricEvent]

  def timeSeriesHistory(): TimeSeries[Boolean] = {
    val tsSeq: List[TSEntry[Boolean]] = history().map(history =>
      TSEntry(history.time, history.event, 1) //1 as the history is already in order
    )
    TimeSeries.ofOrderedEntriesUnsafe(tsSeq)
  }

  def active(after: Long = Long.MinValue, before: Long = Long.MaxValue): Boolean
  def aliveAt(time: Long, window: Long = Long.MaxValue): Boolean

  lazy val numCreations: Long = history().count(f => f.event)
  lazy val numDeletions: Long = history().count(f => !f.event)
}

/**
  * # HistoricEvent
  *
  * `HistoricEvent(time: Long, event: Boolean)`
  *  : Case class for encoding additions and deletions
  *
  * This class is returned by the history access methods of [`EntityVisitor`](com.raphtory.graph.visitor.EntityVisitor).
  *  ## Parameters
  *
  *  `time: Long`
  *    : timestamp of event
  *
  *  `event: Boolean`
  *    : `true` if event is an addition or `false` if event is a deletion
  *
  * ```{seealso}
  * [](com.raphtory.graph.visitor.EntityVisitor),
  * [](com.raphtory.graph.visitor.Vertex),
  * [](com.raphtory.graph.visitor.Edge)
  * `
  */
case class HistoricEvent(time: Long, event: Boolean)
