package com.raphtory.core.model.graphentities

import com.raphtory.core.analysis.API.VertexVisitor
import com.raphtory.core.model.communication.{VertexMessage, VertexMutliQueue}
import com.raphtory.core.storage.{EntityStorage, VertexHistoryPoint, VertexPropertyPoint}
import com.raphtory.core.utils.{EntityRemovedAtTimeException, PushedOutOfGraphException, StillWithinLiveGraphException, Utils}

import scala.collection.mutable
import scala.collection.parallel.mutable.ParSet
import scala.collection.parallel.mutable.ParTrieMap

/** Companion Vertex object (extended creator for storage loads) */
object Vertex {
  def apply(routerID:Int, creationTime : Long, vertexId : Int, previousState : mutable.TreeMap[Long, Boolean], properties : ParTrieMap[String, Property]) = {
    val v = new Vertex(routerID,creationTime, vertexId, initialValue = true)
    v.previousState   = previousState
    //v.associatedEdges = associatedEdges
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
    new Vertex(-1,closestTime,id.toInt,value)
  }

}

class Vertex(routerID:Int,msgTime: Long, val vertexId: Int, initialValue: Boolean) extends Entity(routerID,msgTime, initialValue) {
  var incomingProcessing =  ParTrieMap[Long, Edge]() //Map of edges for the current view of the vertex
  var outgoingProcessing=  ParTrieMap[Long, Edge]()

  var incomingEdges  = ParTrieMap[Long, Edge]() //Map of all edges associated with the vertex
  var outgoingEdges  = ParTrieMap[Long, Edge]()

  var multiQueue = new VertexMutliQueue() //Map of queues for all ongoing processing
  var computationValues = ParTrieMap[String, Any]() //Partial results kept between supersteps in calculation

  override def getId = vertexId //get the vertexID

  //Functions for adding associated edges to this vertex
  def addIncomingEdge(edge:Edge):Unit = incomingEdges.put(edge.getId,edge)
  def addOutgoingEdge(edge:Edge):Unit = outgoingEdges.put(edge.getId,edge)
  def addAssociatedEdge(edge: Edge): Unit = if(edge.getSrcId==vertexId) addOutgoingEdge(edge) else addIncomingEdge(edge)
  def getOutgoingEdge(id:Long): Option[Edge] = outgoingEdges.get(id)
  def getIncomingEdge(id:Long): Option[Edge] = incomingEdges.get(id)

  //Getters and setters for processing results
  def addCompValue(key:String,value:Any):Unit = computationValues += ((key,value))
  def getCompValue(key:String) = computationValues(key)
  def getOrSet(key:String,value:Any) ={
    if(computationValues.contains(key))
      computationValues(key)
    else{
      computationValues += ((key,value))
      value
    }
  }

  def viewAt(time:Long):Vertex = {
    incomingProcessing = incomingEdges.filter(e=> e._2.aliveAt(time))
    outgoingProcessing = outgoingEdges.filter(e=> e._2.aliveAt(time))
    this
  }

  def viewAtWithWindow(time:Long,windowSize:Long):Vertex = {
    incomingProcessing = incomingEdges.filter(e=> e._2.aliveAtWithWindow(time,windowSize))
    outgoingProcessing = outgoingEdges.filter(e=> e._2.aliveAtWithWindow(time,windowSize))
    this
  }

  override def equals(obj: scala.Any): Boolean = {
    if(obj.isInstanceOf[Vertex]){
      val v2 = obj.asInstanceOf[Vertex] //add associated edges
      if(!(vertexId == v2.vertexId)                ||
         !(previousState.equals(v2.previousState)) ||
         !(oldestPoint.get == v2.oldestPoint.get)  ||
         !(newestPoint.get == newestPoint.get)     ||
         !(properties.equals(v2.properties))       ||
         !(incomingEdges.equals(v2.incomingEdges)) ||
         !(outgoingEdges.equals(v2.outgoingEdges)))
        false
      else true
    }
    else false
  }

  override def toString: String = s"Vertex ID $vertexId \n History $previousState \n //Properties:\n $properties \n"

  def addSavedProperty(property:VertexPropertyPoint,time:Long): Unit ={
    val history = property.history
    var closestTime:Long = 0
    var value = "default"
    for((k,v) <- history){
      if(k<=time)
        if((time-k)<(time-closestTime)) {
          closestTime = k
          value = v
        }
    }
    if(!(value.equals("default")))
      this + (time,property.name,value)
  }
}
