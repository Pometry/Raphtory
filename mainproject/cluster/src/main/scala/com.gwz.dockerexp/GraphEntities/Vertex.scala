package com.gwz.dockerexp.GraphEntities
import scala.collection.mutable.Set
/**
  * Class representing Grpah Vertices
  */

class Vertex(msdId:Int,id:Int,initialValue:Boolean) extends Entity(msdId,initialValue) {
  val vertexId:Long = id
  var associatedEdges = Set[Tuple2[Int,Int]]()

  def addAssociatedEdge(srcId:Int,dstId:Int):Unit= associatedEdges = associatedEdges + ((srcId,dstId))
  def hasAssociatedEdge(srcId:Int,dstId:Int):Boolean= associatedEdges contains ((srcId,dstId))

  override def printProperties():String = s"Vertex $vertexId with properties: \n"+super.printProperties()

}
