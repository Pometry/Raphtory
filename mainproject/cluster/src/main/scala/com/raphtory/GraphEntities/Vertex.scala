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

  var associatedEdges = Set[Tuple2[Int, Int]]()

  def addAssociatedEdge(srcId: Int, dstId: Int): Unit =
    associatedEdges = associatedEdges + ((srcId, dstId))

  def hasAssociatedEdge(srcId: Int, dstId: Int): Boolean =
    associatedEdges contains ((srcId, dstId))

  override def printProperties(): String =
    s"Vertex $vertexId with properties: \n" + super.printProperties()
}
