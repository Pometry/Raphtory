package com.raphtory.api.analysis.visitor

/** Base trait for [[ExplodedEdge]] and [[ExplodedVertex]]
  *  Extends [[EntityVisitor]] adding a `timestamp` property.
  *  Further, all the history access methods take this `timestamp` as their default value for `before`.
  */
trait ExplodedEntityVisitor extends EntityVisitor {

  /** Timestamp for exploded entity */
  def timestamp: Long
  def firstActivityAfter(time: Long = timestamp, strict: Boolean = true): Option[HistoricEvent]
  def lastActivityBefore(time: Long = timestamp, strict: Boolean = true): Option[HistoricEvent]

  override def getPropertyValues[T](
      key: String,
      after: Long = Long.MinValue,
      before: Long = timestamp
  ): Option[Iterable[T]] = super.getPropertyValues[T](key, after, before)

  override def getPropertyHistory[T](
      key: String,
      after: Long = Long.MinValue,
      before: Long = timestamp
  ): Option[Iterable[PropertyValue[T]]]

}
