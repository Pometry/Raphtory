package com.raphtory.graph.visitor

/** Base trait for [[com.raphtory.graph.visitor.ExplodedEdge]] and [[com.raphtory.graph.visitor.ExplodedVertex]]
  *  Extends [`EntityVisitor`](com.raphtory.graph.visitor.EntityVisitor) adding a `timestamp` property.
  *  Further, all the history access methods take this `timestamp` as their default value for `before`.
  */
trait ExplodedEntityVisitor extends EntityVisitor {
  def timestamp: Long

  def firstActivityAfter(time: Long = timestamp, strict: Boolean = true): Option[HistoricEvent]
  def lastActivityBefore(time: Long = timestamp, strict: Boolean = true): Option[HistoricEvent]

  override def getPropertyValues[T](
      key: String,
      after: Long = Long.MinValue,
      before: Long = timestamp
  ): Option[List[T]] = super.getPropertyValues[T](key, after, before)

  override def getPropertyHistory[T](
      key: String,
      after: Long = Long.MinValue,
      before: Long = timestamp
  ): Option[List[(Long, T)]]

}
