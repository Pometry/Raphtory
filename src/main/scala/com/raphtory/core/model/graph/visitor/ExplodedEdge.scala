package com.raphtory.core.model.graph.visitor

import scala.reflect.ClassTag

trait ExplodedEdge {
  def Type()
  def ID():Long
  def src():Long
  def dst():Long
  def getPropertySet(): List[String]
  def getPropertyValue[T: ClassTag](key: String): Option[T]
  def send(data:Any):Unit
  def getTimestamp():Long
}
