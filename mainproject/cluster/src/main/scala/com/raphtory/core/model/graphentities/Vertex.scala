package com.raphtory.core.model.graphentities

import com.raphtory.core.actors.partitionmanager.SavedVertex

import scala.collection.mutable
import scala.collection.parallel.ParSet
import scala.collection.parallel.mutable.ParTrieMap

/**
  * Companion Vertex object (extended creator for storage loads)
  */
object Vertex {
  def apply(routerID:Int, creationTime : Long, vertexId : Int, associatedEdges : ParSet[Edge], previousState : mutable.TreeMap[Long, Boolean], properties : ParTrieMap[String, Property]) = {
    val v = new Vertex(routerID,creationTime, vertexId, initialValue = true, addOnly = false)
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
class Vertex(routerID:Int,msgTime: Long, val vertexId: Int, initialValue: Boolean, addOnly:Boolean)
    extends Entity(routerID,msgTime, initialValue,addOnly) {

  var associatedEdges = ParSet[Edge]()
  var newAssociatedEdges = ParSet[Edge]()

  def addAssociatedEdge(edge: Edge): Unit = {
    associatedEdges += edge
    newAssociatedEdges += edge
  }

  def getNewAssociatedEdges():ParSet[Edge] ={
    val temp = newAssociatedEdges
    newAssociatedEdges = ParSet[Edge]()
    temp
  }

  /*override def printProperties(): String =
    s"Vertex $vertexId with properties: \n" + super.printProperties()*/
  override def getId = vertexId

  override def equals(obj: scala.Any): Boolean = {
    if(obj.isInstanceOf[Vertex]){
      val v2 = obj.asInstanceOf[Vertex] //add associated edges
      if((vertexId == v2.vertexId) &&(previousState.equals(v2.previousState)) && (oldestPoint.get == v2.oldestPoint.get) && (newestPoint.get == oldestPoint.get) && (properties.size == v2.properties.size)){
        for((key,prop) <- properties){
          if(!prop.equals(v2.properties.getOrElse(key,null))){
            return false
          }
        }
      }
      return true
    }
    false
  }


}
