package com.raphtory.internals.storage.arrow.entities

import com.raphtory.api.analysis.visitor.EntityVisitor
import com.raphtory.api.analysis.visitor.HistoricEvent
import com.raphtory.api.analysis.visitor.PropertyValue
import com.raphtory.arrowcore.model.Entity
import com.raphtory.arrowcore.model.{Edge => ArrEdge}
import com.raphtory.arrowcore.model.{Vertex => ArrVertex}
import com.raphtory.internals.storage.arrow.ArrowEntityStateRepository
import com.raphtory.internals.storage.arrow.Field
import com.raphtory.internals.storage.arrow.Prop
import com.raphtory.internals.storage.arrow.PropAccess
import com.raphtory.internals.storage.arrow.RichEdge
import com.raphtory.internals.storage.arrow.RichVertex

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.reflect.ClassTag

trait ArrowExEntity extends EntityVisitor {

  protected def repo: ArrowEntityStateRepository

  protected def entity: Entity

  /** Return the type of the entity */
  override def Type: String = ???

  /** Return the next event (addition or deletion) after the given timestamp `time` as an
    * [[com.raphtory.api.analysis.visitor.HistoricEvent HistoricEvent]]. This is wrapped in an option as
    * it is possible that no activity occurred after the given time. An optional `strict` Boolean argument is also
    * available which allows events exactly at the given time to be returned if set to `false`.
    *
    * @param time   The time after which to return the next occurring event
    * @param strict Whether events occurring exactly at the given time should be excluded or not
    * @return an optional historic event containing the event if one exists
    */
  override def firstActivityAfter(time: Long, strict: Boolean): Option[HistoricEvent] = ???

  /** Return the last event (addition or deletion) before the given timestamp `time` as an
    * [[com.raphtory.api.analysis.visitor.HistoricEvent HistoricEvent]].  The result is wrapped in an option as it is
    * possible that no activity occurred before the given time. An optional `strict` Boolean argument is also
    * available which allows events exactly at the given time to be returned if set to `false`.
    *
    * @param time   The time before which to return the latest occurring event
    * @param strict Whether events occurring exactly at the given time should be excluded or not
    * @return an optional historic event containing the event if one exists
    */
  override def lastActivityBefore(time: Long, strict: Boolean): Option[HistoricEvent] = ???

  /** Return the most recent event (addition or deletion) in the current view as a
    * [[com.raphtory.api.analysis.visitor.HistoricEvent HistoricEvent]]
    */
  override def latestActivity(): HistoricEvent = ???

  /** Return the first event (addition or deltion) in the current view as a
    * [[com.raphtory.api.analysis.visitor.HistoricEvent HistoricEvent]]
    */
  override def earliestActivity(): HistoricEvent = ???

  /** Return a list of keys for available properties for the entity */
  override def getPropertySet(): List[String]

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
    * @param key    Name of property
    * @param after  Only consider addition events in the current view that happened no earlier than time `after`
    * @param before Only consider addition events in the current view that happened no later than time `before`
    */
  override def getPropertyHistory[T](key: String, after: Long, before: Long): Option[Iterable[PropertyValue[T]]] =
    this match {
      case vertex: ArrowExVertex =>
        if (!isField(key)) {
          implicit val PROP: Prop[T] = Prop.runtime[T](vertex.entity)
          val arrV                   = vertex.vertex
          historyProps(arrV.prop[T](key), after, before)
        }
        else {
          implicit val FIELD: Field[T] = com.raphtory.internals.storage.arrow.Field.runtime[T]
          Some(
                  List(
                          PropertyValue(
                                  vertex.vertex.getCreationTime,
                                  vertex.vertex.getCreationTime,
                                  vertex.vertex.field[T](key).get
                          )
                  )
          )
        }
      case edge: ArrowExEdge     =>
        if (!isField(key)) {
          implicit val PROP: Prop[T] = Prop.runtime[T](edge.entity)
          val arrE                   = edge.entity.asInstanceOf[ArrEdge]
          historyProps(arrE.prop[T](key), after, before).map(_.toVector)
        }
        else {
          implicit val FIELD: Field[T] = com.raphtory.internals.storage.arrow.Field.runtime[T]
          Some(
                  List(
                          PropertyValue(
                                  edge.edge.getCreationTime,
                                  edge.edge.getCreationTime,
                                  edge.edge.field[T](key).get
                          )
                  )
          )
        }
    }

  /**
    * check if the property is field or versioned property
    * @param key
    * @return
    */
  def isField(key: String): Boolean =
    this match {
      case edge: ArrowExEdge     =>
        entity.getRaphtory.getPropertySchema.nonversionedEdgeProperties().asScala.map(_.name()).exists(_ == key)
      case vertex: ArrowExVertex =>
        entity.getRaphtory.getPropertySchema.nonversionedVertexProperties().asScala.map(_.name()).exists(_ == key)
    }

  private def historyProps[T](addVertexProps: PropAccess[T], after: Long, before: Long) = {
    val hist = addVertexProps.list
      .filter { case (_, t) => t >= after && t <= before }
      .map { case (v, t) => PropertyValue(t, t, v) }

    Some(hist)
  }

  /** Set algorithmic state for this entity. Note that for edges, algorithmic state is stored locally to the vertex endpoint
    * which sets this state (default being the source node when set during an edge step).
    *
    * @param key   key to use for setting value
    * @param value new value for state
    */
  override def setState(key: String, value: Any): Unit =
    repo.setState(entity.getLocalId, key, value)

  /** Retrieve value from algorithmic state. Note that for edges, algorithmic state is stored locally to the vertex endpoint
    * which sets this state (default being the source node when set during an edge step).
    *
    * @tparam `T` value type for state
    * @param key               key to use for retrieving state
    * @param includeProperties set this to `true` to fall-through to vertex properties if `key` is not found
    */
  override def getState[T](key: String, includeProperties: Boolean): T =
    repo.getState(entity.getLocalId, key).get

  /** Retrieve value from algorithmic state if it exists or return a default value otherwise. Note that for edges,
    * algorithmic state is stored locally to the vertex endpoint which set this state (default being the source node
    * when set during an edge step).
    *
    * @tparam `T` value type for state
    * @param key               key to use for retrieving state
    * @param value             default value to return if state does not exist
    * @param includeProperties set this to `true` to fall-through to entity properties
    *                          if `key` is not found in algorithmic state
    */
  override def getStateOrElse[T](key: String, value: T, includeProperties: Boolean): T =
    repo.getStateOrElse(entity.getLocalId, key, value) // TODO: implement include properties

  /** Checks if algorithmic state with key `key` exists. Note that for edges, algorithmic state is stored locally to
    * the vertex endpoint which set this state (default being the source node when set during an edge step).
    *
    * @param key               state key to check
    * @param includeProperties Set this to `true` to fall-through to vertex properties if `key` is not found.
    *                          If set, this function only returns `false` if `key` is not included in either algorithmic state
    *                          or entity properties
    */
  override def containsState(key: String, includeProperties: Boolean): Boolean =
    repo.getState(entity.getLocalId, key).isDefined || (includeProperties && getProperty(key).isDefined)

  /** Retrieve value from algorithmic state if it exists or set this state to a default value and return otherwise. Note that for edges,
    * algorithmic state is stored locally to the vertex endpoint which set this state (default being the source node
    * when set during an edge step).
    *
    * @tparam `T` value type for state
    * @param key               key to use for retrieving state
    * @param value             default value to set and return if state does not exist
    * @param includeProperties set this to `true` to fall-through to vertex properties
    *                          if `key` is not found in algorithmic state. State is only set if this is also not found.
    */
  override def getOrSetState[T](key: String, value: T, includeProperties: Boolean): T =
    repo.getOrSetState(entity.getLocalId, key, value)

  /** Append new value to existing array or initialise new array if state does not exist. Note that for edges,
    * algorithmic state is stored locally to the vertex endpoint which set this state (default being the source node
    * when set during an edge step).
    * The value type of the state is assumed to be `Array[T]` if the state already exists.
    *
    * @tparam `T` value type for state (needs to have a `ClassTag` available due to Scala `Array` implementation)
    * @param key   key to use for retrieving state
    * @param value value to append to state
    */
  override def appendToState[T: ClassTag](key: String, value: T): Unit = ???

  override def history(): List[HistoricEvent] = List.empty

  /** Return `true` if any event (addition or deletion) occurred during the time window starting at
    * `after` and ending at `before`. Otherwise returns `false`.
    *
    * @param after  inclusive lower bound for the time window (defaults to oldest data point)
    * @param before inclusive upper bound for the time window (defaults to newest data point)
    */
  override def active(after: Long, before: Long): Boolean =
    aliveAtWithWindow(after, before)

  /** Check if the entity is currently alive (i.e, the last event was an addition) at time `time`.
    *
    * @param time   time point to check
    * @param window If a value for `window` is given, the entity is considered as having been deleted if more
    *               than `window` time has passed since the last addition event.
    */
  override def aliveAt(time: Long, window: Long): Boolean =
    aliveAtWithWindow(time - window + 1, time)

  private def aliveAtWithWindow(after: Long, before: Long) =
    entity.getCreationTime >= after && entity.getCreationTime <= before
}
