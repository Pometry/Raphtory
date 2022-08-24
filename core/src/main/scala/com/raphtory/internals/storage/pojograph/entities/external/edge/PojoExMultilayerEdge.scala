package com.raphtory.internals.storage.pojograph.entities.external.edge

import com.raphtory.api.analysis.visitor.EntityVisitor
import com.raphtory.api.analysis.visitor.HistoricEvent
import com.raphtory.api.analysis.visitor.IndexedValue
import com.raphtory.api.analysis.visitor.PropertyValue
import com.raphtory.api.analysis.visitor.TimePoint
import com.raphtory.internals.storage.pojograph.PojoGraphLens

import scala.reflect.ClassTag

private[pojograph] class PojoExMultilayerEdge(
    val timestamp: Long,
    override val ID: (Long, Long),
    override val src: (Long, Long),
    override val dst: (Long, Long),
    protected val edge: EntityVisitor,
    override val view: PojoGraphLens
) extends PojoExDirectedEdgeBase[PojoExMultilayerEdge, (Long, Long)]
        with PojoExEdgeBase[(Long, Long)] {

  override def Type: String = edge.Type

  override def firstActivityAfter(time: Long, strict: Boolean): Option[HistoricEvent] =
    edge.firstActivityAfter(time, strict)

  override def lastActivityBefore(time: Long, strict: Boolean): Option[HistoricEvent] =
    edge.lastActivityBefore(time, strict)

  override def latestActivity(): HistoricEvent = edge.latestActivity()

  override def earliestActivity(): HistoricEvent = edge.earliestActivity()

  override def getPropertySet(): List[String] = edge.getPropertySet()

  override def getPropertyAt[T](key: String, time: Long): Option[T] = edge.getPropertyAt(key, time)

  override def getPropertyHistory[T](
      key: String,
      after: Long,
      before: Long = timestamp
  ): Option[Iterable[PropertyValue[T]]] = edge.getPropertyHistory[T](key, after, before)

  override def history(): List[HistoricEvent] = edge.history()

  override def active(after: Long, before: Long): Boolean = edge.active(after, before)

  override def aliveAt(time: Long, window: Long): Boolean = edge.aliveAt(time, window)

  override def reversed: PojoExMultilayerEdge =
    new PojoExMultilayerEdge(timestamp, ID, dst, src, edge, view)

  override type Eundir = PojoExEdgeBase[(Long, Long)]

  override def combineUndirected(other: PojoExMultilayerEdge, asInEdge: Boolean): PojoExMultilayerInOutEdge =
    if (isIncoming)
      new PojoExMultilayerInOutEdge(this, other, asInEdge)
    else
      new PojoExMultilayerInOutEdge(other, this, asInEdge)

  override def start: IndexedValue = TimePoint.first(timestamp)

  override def end: IndexedValue = TimePoint.last(timestamp)

  override def setState(key: String, value: Any): Unit = ???

  /** Retrieve value from algorithmic state
    *
    * @tparam `T` value type for state
    * @param key               key to use for retrieving state
    * @param includeProperties set this to `true` to fall-through to vertex properties if `key` is not found
    */
  override def getState[T](key: String, includeProperties: Boolean): T = ???

  /** Retrieve value from algorithmic state if it exists or return a default value otherwise
    *
    * @tparam `T` value type for state
    * @param key               key to use for retrieving state
    * @param value             default value to return if state does not exist
    * @param includeProperties set this to `true` to fall-through to vertex properties
    *                          if `key` is not found in algorithmic state
    */
override def getStateOrElse[T](key: String, value: T, includeProperties: Boolean): T = ???

  /** Checks if algorithmic state with key `key` exists
    *
    * @param key               state key to check
    * @param includeProperties Set this to `true` to fall-through to vertex properties if `key` is not found.
    *                          If set, this function only returns `false` if `key` is not included in either algorithmic state
    *                          or vertex properties
    */
  override def containsState(key: String, includeProperties: Boolean): Boolean = ???

  /** Retrieve value from algorithmic state if it exists or set this state to a default value and return otherwise
    *
    * @tparam `T` value type for state
    * @param key               key to use for retrieving state
    * @param value             default value to set and return if state does not exist
    * @param includeProperties set this to `true` to fall-through to vertex properties
    *                          if `key` is not found in algorithmic state. State is only set if this is also not found.
    */
  override def getOrSetState[T](key: String, value: T, includeProperties: Boolean): T = ???

  /** Append new value to existing array or initialise new array if state does not exist
    * The value type of the state is assumed to be `Array[T]` if the state already exists.
    *
    * @tparam `T` value type for state (needs to have a `ClassTag` available due to Scala `Array` implementation)
    * @param key   key to use for retrieving state
    * @param value value to append to state
    */
  override def appendToState[T: ClassTag](key: String, value: T): Unit = ???
}

private[pojograph] class PojoExMultilayerInOutEdge(
    in: PojoExMultilayerEdge,
    out: PojoExMultilayerEdge,
    asInEdge: Boolean
) extends PojoExInOutEdgeBase[PojoExMultilayerInOutEdge, PojoExMultilayerEdge, (Long, Long)](in, out, asInEdge)
        with PojoExEdgeBase[(Long, Long)] {

  /** Timestamp for exploded entity */
  def timestamp: Long = in.timestamp

  override def setState(key: String, value: Any): Unit = ???

  /** Retrieve value from algorithmic state
    *
    * @tparam `T` value type for state
    * @param key               key to use for retrieving state
    * @param includeProperties set this to `true` to fall-through to vertex properties if `key` is not found
    */
  override def getState[T](key: String, includeProperties: Boolean): T = ???

  /** Retrieve value from algorithmic state if it exists or return a default value otherwise
    *
    * @tparam `T` value type for state
    * @param key               key to use for retrieving state
    * @param value             default value to return if state does not exist
    * @param includeProperties set this to `true` to fall-through to vertex properties
    *                          if `key` is not found in algorithmic state
    */
override def getStateOrElse[T](key: String, value: T, includeProperties: Boolean): T = ???

  /** Checks if algorithmic state with key `key` exists
    *
    * @param key               state key to check
    * @param includeProperties Set this to `true` to fall-through to vertex properties if `key` is not found.
    *                          If set, this function only returns `false` if `key` is not included in either algorithmic state
    *                          or vertex properties
    */
  override def containsState(key: String, includeProperties: Boolean): Boolean = ???

  /** Retrieve value from algorithmic state if it exists or set this state to a default value and return otherwise
    *
    * @tparam `T` value type for state
    * @param key               key to use for retrieving state
    * @param value             default value to set and return if state does not exist
    * @param includeProperties set this to `true` to fall-through to vertex properties
    *                          if `key` is not found in algorithmic state. State is only set if this is also not found.
    */
  override def getOrSetState[T](key: String, value: T, includeProperties: Boolean): T = ???

  /** Append new value to existing array or initialise new array if state does not exist
    * The value type of the state is assumed to be `Array[T]` if the state already exists.
    *
    * @tparam `T` value type for state (needs to have a `ClassTag` available due to Scala `Array` implementation)
    * @param key   key to use for retrieving state
    * @param value value to append to state
    */
  override def appendToState[T: ClassTag](key: String, value: T): Unit = ???
}
