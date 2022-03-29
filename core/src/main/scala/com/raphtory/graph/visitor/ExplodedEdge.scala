package com.raphtory.graph.visitor

/**
  * {s}`ExplodedEdge`
  *   : trait representing a view of an edge at a given point in time
  *
  * ## Attributes
  *
  * {s}`Type(): String`
  *   : type of the underlying edge
  *
  * {s}`ID(): Long`
  *   : ID of the underlying edge
  *
  * {s}`src(): Long`
  *   : ID of the source vertex of the edge
  *
  * {s}`dst(): Long`
  *   : ID of the destination vertex of the edge
  *
  * {s}`timestamp(): Long`
  *   : time point at which we are viewing the edge
  *
  * ## Methods
  *
  * {s}`getPropertySet(): List[String]`
  *   : list available edge property names
  *
  * {s}`getPropertyValue[T](key: String): Option[T]`
  *   : get value for edge property
  *     {s}`T`
  *       : value type of property
  *
  *     {s}`key: String`
  *       : name of property
  *
  *     This method gets the value for the property {s}`key` at time {s}`timestamp()`
  *     of the underlying edge.
  *
  * {s}`send(data: Any): Unit`
  *   : send message to destination vertex of the edge
  *
  *     {s}`data: Any`
  *       : message data to send
  */
trait ExplodedEdge extends Edge {
  def timestamp: Long
}
