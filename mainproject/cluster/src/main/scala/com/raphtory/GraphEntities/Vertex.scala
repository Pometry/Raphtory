package com.raphtory.GraphEntities

import scala.collection.mutable.Set

/** *
  * Class representing Graph Vertices
  *
  * @param msdId
  * @param vertexId
  * @param initialValue
  */
class Vertex(msdId: Int, val vertexId: Int, initialValue: Boolean)
    extends Entity(msdId, initialValue) {

  var associatedEdges = Set[Edge]()

  def addAssociatedEdge(edge: Edge): Unit =
    associatedEdges += edge

  override def printProperties(): String =
    s"Vertex $vertexId with properties: \n" + super.printProperties()
}
