package com.raphtory.graph.visitor

import com.raphtory.graph.visitor.PropertyMergeStrategy.PropertyMerge

/** {s}`ExplodedEntityVisitor`
  *   : Base trait for [{s}`ExplodedEdge`](com.raphtory.graph.visitor.ExplodedEdge) and
  *     [{s}`ExplodedVertex`](com.raphtory.graph.visitor.ExplodedVertex)
  *
  *  Extends [{s}`EntityVisitor`](com.raphtory.graph.visitor.EntityVisitor) adding a {s}`timestamp` property.
  *  Further, all the history access methods take this {s}`timestamp` as their default value for {s}`before`.
  */
trait ExplodedEntityVisitor extends EntityVisitor {
  def timestamp: Long

  def firstActivityAfter(time: Long = timestamp): Option[HistoricEvent]
  def lastActivityBefore(time: Long = timestamp): Option[HistoricEvent]

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
