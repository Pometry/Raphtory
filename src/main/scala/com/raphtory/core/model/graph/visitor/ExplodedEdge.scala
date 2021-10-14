package com.raphtory.core.model.graph.visitor

trait ExplodedEdge {
  def Type()
  def ID():Long
  def src():Long
  def dst():Long
  def getPropertySet(): List[String]
  def getPropertyValue(key: String): Option[Any]
  def send(data:Any):Unit
}
