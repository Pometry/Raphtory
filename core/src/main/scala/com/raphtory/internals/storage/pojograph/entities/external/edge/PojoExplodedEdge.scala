package com.raphtory.internals.storage.pojograph.entities.external.edge

import com.raphtory.api.analysis.visitor.IndexedValue
import com.raphtory.api.analysis.visitor.TimePoint
import com.raphtory.internals.storage.pojograph.PojoGraphLens
import com.raphtory.internals.storage.pojograph.entities.external.PojoExEntity
import com.raphtory.internals.storage.pojograph.entities.external.vertex._
import com.raphtory.internals.storage.pojograph.entities.internal.PojoEdge

import scala.reflect.ClassTag

private[pojograph] class PojoExplodedEdge(
    val edge: PojoEdge,
    override val view: PojoGraphLens,
    override val ID: Long,
    val timePoint: IndexedValue
) extends PojoExEntity(edge, view, timePoint, timePoint)
        with PojoExDirectedEdgeBase[PojoExplodedEdge, Long]
        with PojoExplodedEdgeBase[Long] {

  override type Eundir = PojoExplodedEdgeBase[Long]

  override def reversed: PojoReversedExplodedEdge =
    PojoReversedExplodedEdge.fromExplodedEdge(this)

  override def src: Long = edge.getSrcId

  override def dst: Long = edge.getDstId

  override def timestamp: Long = timePoint.time

  override def combineUndirected(other: PojoExplodedEdge, asInEdge: Boolean): PojoExplodedInOutEdge =
    if (isIncoming)
      new PojoExplodedInOutEdge(this, other, asInEdge)
    else
      new PojoExplodedInOutEdge(other, this, asInEdge)

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

private[pojograph] object PojoExplodedEdge {

  def fromEdge(pojoExEdge: PojoExEdge, timePoint: IndexedValue): PojoExplodedEdge =
    new PojoExplodedEdge(
            pojoExEdge.edge,
            pojoExEdge.view,
            pojoExEdge.ID,
            timePoint
    )
}

private[pojograph] class PojoExplodedInOutEdge(
    in: PojoExplodedEdge,
    out: PojoExplodedEdge,
    asInEdge: Boolean = false
) extends PojoExInOutEdgeBase[PojoExplodedInOutEdge, PojoExplodedEdge, Long](in, out, asInEdge)
        with PojoExplodedEdgeBase[Long] {

  override def timePoint: IndexedValue = in.timePoint

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

private[pojograph] class PojoReversedExplodedEdge(
    objectEdge: PojoEdge,
    override val view: PojoGraphLens,
    override val ID: Long,
    override val timePoint: IndexedValue
) extends PojoExplodedEdge(objectEdge, view, ID, timePoint) {
  override def src: Long = edge.getDstId

  override def dst: Long = edge.getSrcId
}

private[pojograph] object PojoReversedExplodedEdge {

  def fromExplodedEdge(pojoExEdge: PojoExplodedEdge): PojoReversedExplodedEdge = {
    val id = if (pojoExEdge.ID == pojoExEdge.src) pojoExEdge.dst else pojoExEdge.src
    new PojoReversedExplodedEdge(
            pojoExEdge.edge,
            pojoExEdge.view,
            id,
            pojoExEdge.timePoint
    )
  }

  def fromReversedEdge(pojoExReversedEdge: PojoExReversedEdge, timestamp: IndexedValue): PojoReversedExplodedEdge =
    new PojoReversedExplodedEdge(
            pojoExReversedEdge.edge,
            pojoExReversedEdge.view,
            pojoExReversedEdge.ID,
            timestamp
    )
}
