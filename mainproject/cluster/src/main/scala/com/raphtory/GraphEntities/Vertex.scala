package com.raphtory.GraphEntities

import scala.collection.mutable
import scala.collection.mutable.Set

/** *
  * Class representing Graph Vertices
  *
  * @param msdId
  * @param vertexId
  * @param initialValue
  * @param addOnly
  */
class Vertex(msdId: Int, val vertexId: Int, initialValue: Boolean, addOnly:Boolean)
    extends Entity(msdId, initialValue,addOnly) {

  var associatedEdges = mutable.LinkedHashSet[Edge]()

  def addAssociatedEdge(edge: Edge): Unit =
    associatedEdges add edge

  /*override def printProperties(): String =
    s"Vertex $vertexId with properties: \n" + super.printProperties()*/
}
