package com.raphtory.api.analysis.visitor

import PropertyMergeStrategy.PropertyMerge

import scala.math.Ordered.orderingToOrdered
import scala.reflect.ClassTag

/** Common base class for [[Edge]] and [[Vertex]]
  *
  * The `EntityVisitor` class defines the interface for accessing properties (set when the graph is constructed)
  * and historic activity for vertices and edges.
  *
  * @see [[Edge]], [[Vertex]], [[PropertyMergeStrategy]], [[HistoricEvent]]
  */
abstract class EntityVisitor {

  /** Return the type of the entity */
  def Type: String

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
  //TODO: Add customised merge strategies and figure out how this should work
  def getPropertyAt[T](key: String, time: Long): Option[T] =
    getPropertyHistory[T](key, time, time).map(h => PropertyMergeStrategy.latest[T](h))

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
  ): Option[Iterable[T]] =
    getPropertyHistory[T](key, after, before) match {
      case Some(history) =>
        Some(PropertyMergeStrategy.sequence(history))
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
  ): Option[Iterable[PropertyValue[T]]]

  /** Set algorithmic state for this entity. Note that for edges, algorithmic state is stored locally to the vertex endpoint
    * which sets this state (default being the source node when set during an edge step).
    * @param key key to use for setting value
    * @param value new value for state
    */
  def setState(key: String, value: Any): Unit

  /** Retrieve value from algorithmic state. Note that for edges, algorithmic state is stored locally to the vertex endpoint
    * which sets this state (default being the source node when set during an edge step).
    * @tparam `T` value type for state
    * @param key key to use for retrieving state
    * @param includeProperties set this to `true` to fall-through to vertex properties if `key` is not found
    */
  def getState[T](key: String, includeProperties: Boolean = false): T

  /** Retrieve value from algorithmic state if it exists or return a default value otherwise. Note that for edges,
    * algorithmic state is stored locally to the vertex endpoint which set this state (default being the source node
    * when set during an edge step).
    * @tparam `T` value type for state
    * @param key key to use for retrieving state
    * @param value default value to return if state does not exist
    * @param includeProperties set this to `true` to fall-through to entity properties
    *                          if `key` is not found in algorithmic state
    */
  def getStateOrElse[T](key: String, value: T, includeProperties: Boolean = false): T

  /** Checks if algorithmic state with key `key` exists. Note that for edges, algorithmic state is stored locally to
    * the vertex endpoint which set this state (default being the source node when set during an edge step).
    * @param key state key to check
    * @param includeProperties Set this to `true` to fall-through to vertex properties if `key` is not found.
    *         If set, this function only returns `false` if `key` is not included in either algorithmic state
    *         or entity properties
    */
  def containsState(key: String, includeProperties: Boolean = false): Boolean

  /** Retrieve value from algorithmic state if it exists or set this state to a default value and return otherwise. Note that for edges,
    * algorithmic state is stored locally to the vertex endpoint which set this state (default being the source node
    * when set during an edge step).
    * @tparam `T` value type for state
    * @param key key to use for retrieving state
    * @param value default value to set and return if state does not exist
    * @param includeProperties set this to `true` to fall-through to vertex properties
    *                          if `key` is not found in algorithmic state. State is only set if this is also not found.
    */
  def getOrSetState[T](key: String, value: T, includeProperties: Boolean = false): T

  /** Append new value to existing array or initialise new array if state does not exist. Note that for edges,
    * algorithmic state is stored locally to the vertex endpoint which set this state (default being the source node
    * when set during an edge step).
    * The value type of the state is assumed to be `Array[T]` if the state already exists.
    * @tparam `T` value type for state (needs to have a `ClassTag` available due to Scala `Array` implementation)
    * @param key key to use for retrieving state
    * @param value value to append to state
    */
  def appendToState[T: ClassTag](key: String, value: T): Unit

  def history(): List[HistoricEvent]

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
case class HistoricEvent(time: Long, index: Long, event: Boolean = true) extends IndexedValue {

  override def sameValue(that: IndexedValue): Boolean =
    that match {
      case HistoricEvent(otime, _, oevent) => otime == time && oevent == event
      case _                               => false
    }
}

//object HistoricEvent {
//  implicit val ordering: Ordering[IndexedValue] = IndexedValue.ordering[IndexedValue]
//}

/** Case class for encoding property value updates
  *
  * @param time Timestamp of update
  * @param index Index of update
  * @param value new property value
  */
case class PropertyValue[A](time: Long, index: Long, value: A = null) extends IndexedValue {

  override def sameValue(that: IndexedValue): Boolean =
    that match {
      case PropertyValue(otime, _, ovalue) => otime == time && ovalue == value
      case _                               => false
    }
}

trait IndexedValue extends Ordered[IndexedValue] {
  def time: Long
  def index: Long

  def next: TimePoint =
    if (this == TimePoint.last) TimePoint.last
    else if (index < Long.MaxValue) TimePoint(time, index + 1)
    else TimePoint(time + 1, Long.MinValue)

  def previous: TimePoint =
    if (this == TimePoint.first) TimePoint.first
    else if (index > Long.MinValue) TimePoint(time, index - 1)
    else TimePoint(time - 1, Long.MaxValue)

  def compare(that: IndexedValue): Int = (time, index) compare (that.time, that.index)
  def sameValue(that: IndexedValue): Boolean
}

case class TimePoint(time: Long, index: Long) extends IndexedValue {
  override def sameValue(that: IndexedValue): Boolean = that.time == time
}

object TimePoint {
  val first: TimePoint                  = TimePoint(Long.MinValue, Long.MinValue)
  def first(timestamp: Long): TimePoint = TimePoint(timestamp, Long.MinValue)
  val last: TimePoint                   = TimePoint(Long.MaxValue, Long.MaxValue)
  def last(timestamp: Long): TimePoint  = TimePoint(timestamp, Long.MaxValue)
}
