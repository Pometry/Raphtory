package com.raphtory.graph.visitor

import com.raphtory.graph.visitor.PropertyMergeStrategy.PropertyMerge

trait ExplodedEntityVisitor extends EntityVisitor {
  def Type(): String
  def timestamp: Long

  def firstActivityAfter(time: Long = timestamp): HistoricEvent
  def lastActivityBefore(time: Long = timestamp): HistoricEvent

  def getPropertySet(): List[String]

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
