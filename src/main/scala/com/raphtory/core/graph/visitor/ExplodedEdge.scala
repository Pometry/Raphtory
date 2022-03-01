package com.raphtory.core.graph.visitor

trait ExplodedEdge {
  def Type()
  def ID(): Long
  def src(): Long
  def dst(): Long
  def getPropertySet(): List[String]
  def getPropertyValue[T](key: String): Option[T]
  def send(data: Any): Unit
  def getTimestamp(): Long
}
