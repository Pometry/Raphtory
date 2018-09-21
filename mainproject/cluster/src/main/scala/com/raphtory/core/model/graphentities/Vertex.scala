package com.raphtory.core.model.graphentities

import com.raphtory.core.storage.{VertexHistoryPoint, VertexPropertyPoint}

import scala.collection.mutable
import scala.collection.parallel.ParSet
import scala.collection.parallel.mutable.ParTrieMap

/**
  * Companion Vertex object (extended creator for storage loads)
  */
object Vertex {
  def apply(routerID:Int, creationTime : Long, vertexId : Int, associatedEdges : ParTrieMap[Long, Edge], previousState : mutable.TreeMap[Long, Boolean], properties : ParTrieMap[String, Property]) = {
    val v = new Vertex(routerID,creationTime, vertexId, initialValue = true, addOnly = false)
    v.previousState   = previousState
    v.associatedEdges = associatedEdges
    v.properties      = properties
    v
  }

  def apply(saved:VertexHistoryPoint,time:Long) = {
    val id = saved.id
    val history = saved.history
    var closestTime:Long = 0
    var value = false
    for((k,v) <- history){
      if(k<=time)
        if((time-k)<(time-closestTime)) {
          closestTime = k
          value = v
        }
    }
    new Vertex(-1,closestTime,id.toInt,value,false)
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

  var associatedEdges    = ParTrieMap[Long, Edge]()

  def addAssociatedEdge(edge: Edge): Unit = {
    associatedEdges.put(edge.getId,edge)
  }

  /*override def printProperties(): String =
    s"Vertex $vertexId with properties: \n" + super.printProperties()*/
  override def getId = vertexId

  def addSavedProperty(property:VertexPropertyPoint,time:Long): Unit ={
    val history = property.history
    var closestTime:Long = 0
    var value = ""
    for((k,v) <- history){
      if(k<=time)
        if((time-k)<(time-closestTime)) {
          closestTime = k
          value = v
        }
    }
    this + (closestTime,property.name,value)
  }

  override def equals(obj: scala.Any): Boolean = {
    if(obj.isInstanceOf[Vertex]){
      val v2 = obj.asInstanceOf[Vertex] //add associated edges
      if(!(vertexId == v2.vertexId)) {
        false
      }
      else if(!(previousState.equals(v2.previousState))){
        println("Previous State incorrect:")
        println(previousState)
        println(v2.previousState)
        false
      }
      else if(!(oldestPoint.get == v2.oldestPoint.get)){
        println("oldest point incorrect:")
        println(previousState)
        println(v2.previousState)
        println(oldestPoint.get)
        println(v2.oldestPoint.get)
        false
      }
      else if(!(newestPoint.get == newestPoint.get)){
        println("newest point incorrect:")
        println(previousState)
        println(v2.previousState)
        println(newestPoint.get)
        println(v2.newestPoint.get)
        false
      }

      else if(!(properties.equals(v2.properties))){
        println("properties incorrect:")
        println(properties)
        println(v2.properties)
        false
      }

      else if(!(associatedEdges.equals(v2.associatedEdges))){
        println("associated edges incorrect:")
        println(associatedEdges)
        println(v2.associatedEdges)
        false
      }
      else true
    }
    else false

  }

  override def toString: String = {
    s"Vertex ID $vertexId \n History $previousState \n Properties:\n $properties \n Associated Edges: $associatedEdges"
  }

}

//        for((key,prop) <- properties){
//          if(!prop.equals(v2.properties.getOrElse(key,null))){
//            return false
//          }
//        }
