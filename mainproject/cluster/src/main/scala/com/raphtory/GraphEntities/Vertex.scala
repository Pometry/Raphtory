package com.raphtory.GraphEntities

import scala.collection.mutable
import scala.collection.mutable.Set

/** *
  * Class representing Graph Vertices
  *
  * @param msgTime
  * @param vertexId
  * @param initialValue
  * @param addOnly
  */
class Vertex(msgTime: Long, val vertexId: Int, initialValue: Boolean, addOnly:Boolean)
    extends Entity(msgTime, initialValue,addOnly) {

  var associatedEdges = mutable.LinkedHashSet[Edge]()

  def addAssociatedEdge(edge: Edge): Unit =
    associatedEdges add edge

  /*override def printProperties(): String =
    s"Vertex $vertexId with properties: \n" + super.printProperties()*/
  override def getId = vertexId
}
