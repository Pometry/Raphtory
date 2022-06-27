package com.raphtory.api.analysis.visitor

import PropertyMergeStrategy.PropertyMerge
import io.sqooba.oss.timeseries.TimeSeries
import io.sqooba.oss.timeseries.immutable.EmptyTimeSeries
import io.sqooba.oss.timeseries.immutable.TSEntry

/** Common base class for [[Edge]] and [[Vertex]]
  *
  * The `EntityVisitor` class defines the interface for accessing properties (set when the graph is constructed)
  * and historic activity for vertices and edges.
  *
  * @see [[Edge]], [[Vertex]], [[PropertyMergeStrategy]], [[HistoricEvent]]
  */
abstract class EntityVisitor {

  /** Return the type of the entity */
  def Type(): String

  /** Return the next event (addition or deletion) after the given timestamp `time` as an
    * [[com.raphtory.api.analysis.visitor.HistoricEvent HistoricEvent]]. This is wrapped in an option as
    * it is possible that no activity occurred after the given time. An optional `strict` Boolean argument is also
    * available which allows events exactly at the given time to be returned if set to `false`.
    *
    * @param time The time after which to return the next occurring event
    * @param strict Whether events occurring exactly at the given time should be excluded or not
    * @return an optional historic event containing the event if one exists
    */
  def firstActivityAfter(time: Long, strict: Boolean = true): Option[HistoricEvent]

  /** Return the last event (addition or deletion) before the given timestamp `time` as an
    * [[com.raphtory.api.analysis.visitor.HistoricEvent HistoricEvent]].  The result is wrapped in an option as it is
    * possible that no activity occurred before the given time. An optional `strict` Boolean argument is also
    * available which allows events exactly at the given time to be returned if set to `false`.
    *
    * @param time The time before which to return the latest occurring event
    * @param strict Whether events occurring exactly at the given time should be excluded or not
    * @return an optional historic event containing the event if one exists
    */
  def lastActivityBefore(time: Long, strict: Boolean = true): Option[HistoricEvent]

  /** Return the most recent event (addition or deletion) in the current view as a
    * [[com.raphtory.api.analysis.visitor.HistoricEvent HistoricEvent]]
    */
  def latestActivity(): HistoricEvent

  /** Return the first event (addition or deltion) in the current view as a
    * [[com.raphtory.api.analysis.visitor.HistoricEvent HistoricEvent]]
    */
  def earliestActivity(): HistoricEvent

  /** Return a list of keys for available properties for the entity */
  def getPropertySet(): List[String]

  /** Apply a merge strategy to compute the value for property `key` based on its history in the current view.
    * This function returns `None` if the property does not exist or its history in the current view is empty and
    * the computed property value otherwise.
    *
    * @tparam A Value type of property
    *
    * @tparam B Return type of merge strategy
    *
    * @param key Mame of property
    *
    * @param mergeStrategy function to apply to the property history to compute the property value
    *                      The `mergeStrategy` can be an arbitrary function to apply to the property history. However, predefined
    *                      merge strategies for common use cases are provided in
    *                      [[com.raphtory.api.analysis.visitor.PropertyMergeStrategy PropertyMergeStrategy]]. To e.g., compute the
    *                      average value of a property use
    *                      {{{
    *                      import com.raphtory.graph.visitor.PropertyMergeStrategy
    *
    *                      val entity: EntityVisitor
    *                      val average: Double = entity.getProperty[T, Double](key: String, PropertyMergeStrategy.average)
    *                      }}}
    */
  def getProperty[A, B](key: String, mergeStrategy: PropertyMerge[A, B]): Option[B] =
    getPropertyHistory[A](key) match {
      case Some(history) =>
        if (history.isEmpty)
          None
        else
          Some(mergeStrategy(history))
      case None          => None
    }

  /** Get the most recent value for property `key` in the current view.
    * Returns `None` if the property does not exist or its history in the current view is empty.
    *
    * @tparam A Value type of the property
    *
    * @param key name of property
    */
  def getProperty[A](key: String): Option[A] =
    getProperty(key, PropertyMergeStrategy.latest[A])

  /** Apply a merge strategy to compute an aggregate property value given: The property exists and its history in the current view
    * is not empty. If either is false a default value is instead used.
    *
    * @tparam A Value type of property
    *
    * @tparam B Return type of the merge strategy
    *
    * @param key Property name
    *
    * @param otherwise Default value to use if property value does not exist
    *
    * @param mergeStrategy Function to apply to the property history to compute the returned property value.
    */
  def getPropertyOrElse[A, B](key: String, otherwise: B, mergeStrategy: PropertyMerge[A, B]): B =
    getProperty[A, B](key, mergeStrategy) match {
      case Some(value) => value
      case None        => otherwise
    }

  /** Return most recent value of property if it exists or a default value otherwise.
    *
    * @tparam A Value type of property
    *
    * @param key Property name
    *
    * @param otherwise Default value to use if property value does not exist
    */
  def getPropertyOrElse[A](key: String, otherwise: A): A =
    getPropertyOrElse(key, otherwise, PropertyMergeStrategy.latest[A])

  /** Get the most recent value of a property `key` before time `time`
    *
    * @tparam T Value type of property
    *
    * @param key Property name
    *
    * @param time Timestamp for property lookup
    */
  def getPropertyAt[T](key: String, time: Long): Option[T]

  /** Return values for a property `key`. Returns `None` if no property with name `key` exists.
    * Otherwise returns a list of values (which may be empty).
    *
    * @param key Name of property
    *
    * @param after Only consider addition events in the current view that happened after time `after`
    *
    * @param before Only consider addition events in the current view that happened before time `before`
    *
    * This method acts in the same way as `getPropertyHistory` with the difference that only property values
    * are returned.
    */
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

  /** Returns a history of values for the property `key`. Returns `None` if no property with name `key` exists.
    * Otherwise returns a list of `(timestamp, value)` tuples (which may be empty).
    *
    * The exact behaviour depends on the type of the property:
    *
    * In case of a
    * [[com.raphtory.api.input.ImmutableProperty]], the history
    * depends only on the creation time of the property. If the property was created within the current view, the
    * history contains a single tuple with the value of the property and the timestamp given by the creation time
    * of the property. If the property creation time is before the start of the current view, the history contains
    * a single tuple with the value of the property and the timestamp given by the start time of the current view.
    * If the property creation time is after the end of the current view, the history is empty.
    *
    * For the other property types, the history contains
    * the timestamps and values for all addition events within the current view and is empty if the property exists
    * but no addition events occurred in the current view.
    *
    * @note More property types with different value semantics are likely going to be added in the future.
    *
    * @param key Name of property
    * @param after Only consider addition events in the current view that happened no earlier than time `after`
    * @param before Only consider addition events in the current view that happened no later than time `before`
    */
  def getPropertyHistory[T](
      key: String,
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): Option[List[(Long, T)]]

  /** Return values for the property `key`. Returns `None` if no property with name `key` exists.
    * Otherwise returns a TimeSeries (from this library: https://github.com/Sqooba/scala-timeseries-lib)
    * which may be empty.
    * This function utilises the getPropertyHistory function and maps over the list of tuples,
    * retrieving the timestamps to work out the individual TimeSeries entries.
    *
    * @param key name of property
    *
    * @param after Only consider addition events in the current view that happened after time `after`
    *
    * @param before Only consider addition events in the current view that happened before time `before`
    */
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

  /** Return a list of all events (additions or deletions) in the current view. Each event is
    * returned as an [[com.raphtory.api.analysis.visitor.HistoricEvent HistoricEvent]] which
    * encodes whether the event is an addition or deletion and the time of the event.
    */
  def history(): List[HistoricEvent]

  /** Returns a TimeSeries (from this library: https://github.com/Sqooba/scala-timeseries-lib) of the entities [[com.raphtory.api.analysis.visitor.HistoricEvent HistoricEvents]]
    * within the current view by mapping over the `history` function.
    */
  def timeSeriesHistory(): TimeSeries[Boolean] = {
    val tsSeq: List[TSEntry[Boolean]] =
      history().map(history => TSEntry(history.time, history.event, 1) //1 as the history is already in order
      )
    TimeSeries.ofOrderedEntriesUnsafe(tsSeq)
  }

  /** Return `true` if any event (addition or deletion) occurred during the time window starting at
    * `after` and ending at `before`. Otherwise returns `false`.
    *
    * @param after inclusive lower bound for the time window (defaults to oldest data point)
    * @param before inclusive upper bound for the time window (defaults to newest data point)
    */
  def active(after: Long = Long.MinValue, before: Long = Long.MaxValue): Boolean

  /** Check if the entity is currently alive (i.e, the last event was an addition) at time `time`.
    *
    * @param time time point to check
    * @param window If a value for `window` is given, the entity is considered as having been deleted if more
    *               than `window` time has passed since the last addition event.
    */
  def aliveAt(time: Long, window: Long = Long.MaxValue): Boolean

  /** Number of addition events for the entity in the current view */
  lazy val numCreations: Long = history().count(f => f.event)

  /** Number of deletion events for the entity in the current view */
  lazy val numDeletions: Long = history().count(f => !f.event)
}

/** Case class for encoding additions and deletions
  *
  * This class is returned by the history access methods of [[EntityVisitor]].
  *
  * @param time Timestamp of event
  *
  * @param event `true` if event is an addition or `false` if event is a deletion
  *
  * @see [[EntityVisitor]], [[Vertex]], [[Edge]]
  */
case class HistoricEvent(time: Long, event: Boolean)
