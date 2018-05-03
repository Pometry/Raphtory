package com.raphtory.core.model.graphentities

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

/**
  * Companion Vertex object (extended creator for storage loads)
  */
object Vertex {
  def apply(creationTime : Long, vertexId : Int, associatedEdges : mutable.LinkedHashSet[Edge], previousState : mutable.TreeMap[Long, Boolean], properties : TrieMap[String, Property]) = {
    val v = new Vertex(creationTime, vertexId, initialValue = true, addOnly = false)
    v.previousState   = previousState
    v.associatedEdges = associatedEdges
    v.properties      = properties
    v
  }
}


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
