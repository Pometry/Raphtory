package com.raphtory.core.model.graphentities

import com.raphtory.core.analysis.API.VertexVisitor
import com.raphtory.core.model.communication.{VertexMessage, VertexMutliQueue}
import com.raphtory.core.storage.{EntityStorage, VertexHistoryPoint, VertexPropertyPoint}
import com.raphtory.core.utils.{EntityRemovedAtTimeException, PushedOutOfGraphException, StillWithinLiveGraphException, Utils}

import scala.collection.mutable
import scala.collection.parallel.mutable.ParSet
import scala.collection.parallel.mutable.ParTrieMap

/**
  * Companion Vertex object (extended creator for storage loads)
  */
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


/** *
  * Class representing Graph Vertices
  *
  * @param msgTime
  * @param vertexId
  * @param initialValue
  */
class Vertex(routerID:Int,msgTime: Long, val vertexId: Int, initialValue: Boolean) extends Entity(routerID,msgTime, initialValue) {
  var incomingIDs = ParSet[Int]()
  var outgoingIDs = ParSet[Int]()

  var incomingProcessing = ParSet[Int]()
  var outgoingProcessing= ParSet[Int]()

  var incomingEdges  = ParTrieMap[Long, Edge]()
  var outgoingEdges  = ParTrieMap[Long, Edge]()

  private var vertexMultiQueue = new VertexMutliQueue()
  def mutliQueue = vertexMultiQueue
  def setmutliQueue(establishedQueue:VertexMutliQueue) = vertexMultiQueue = establishedQueue

  var computationValues = ParTrieMap[String, Any]()
  def setCompMap(establishedMap:ParTrieMap[String,Any]) = computationValues = establishedMap


  def addAssociatedEdge(edge: Edge): Unit = {
    if(edge.getSrcId==vertexId)
      outgoingEdges.put(edge.getId,edge)
    else
      incomingEdges.put(edge.getId,edge)
  }

  def addIncomingEdge(id:Int):Unit = {
    incomingIDs += id
  }

  def addOutgoingEdge(id:Int):Unit = {
    outgoingIDs += id
  }

  def addCompValue(key:String,value:Any):Unit = {
    computationValues += ((key,value))
  }
  def getCompValue(key:String) = {
    computationValues(key)
  }
  def getOrSet(key:String,value:Any) ={
    if(computationValues.contains(key))
      computationValues(key)
    else{
      computationValues += ((key,value))
      value
    }
  }


  override def getId = vertexId

  def viewAt(time:Long):Vertex = {
    incomingProcessing = incomingIDs.filter(e=> EntityStorage.edges(Utils.getEdgeIndex(e,vertexId)).aliveAt(time))
    outgoingProcessing = outgoingIDs.filter(e=> EntityStorage.edges(Utils.getEdgeIndex(vertexId,e)).aliveAt(time))
    this
  }

  def viewAtWithWindow(time:Long,windowSize:Long):Vertex = {
    incomingProcessing = incomingIDs.filter(e=> EntityStorage.edges(Utils.getEdgeIndex(e,vertexId)).aliveAtWithWindow(time,windowSize))
    outgoingProcessing = outgoingIDs.filter(e=> EntityStorage.edges(Utils.getEdgeIndex(vertexId,e)).aliveAtWithWindow(time,windowSize))
    this
  }

  override def equals(obj: scala.Any): Boolean = {
    if(obj.isInstanceOf[Vertex]){
      val v2 = obj.asInstanceOf[Vertex] //add associated edges
      if(!(vertexId == v2.vertexId) || !(previousState.equals(v2.previousState)) || !(oldestPoint.get == v2.oldestPoint.get) || !(newestPoint.get == newestPoint.get) || !(properties.equals(v2.properties)) || !(incomingEdges.equals(v2.incomingEdges)) || !(outgoingEdges.equals(v2.outgoingEdges)))
        false
      else true
    }
    else false

  }

  override def toString: String = {
    //    s"Vertex ID $vertexId \n History $previousState \n Properties:\n $properties \n Associated Edges: $associatedEdges"
    s"Vertex ID $vertexId \n History $previousState \n //Properties:\n $properties \n"
  }

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

//    if(time > EntityStorage.lastCompressedAt){
//      throw StillWithinLiveGraphException(time)
//    }
//    if(time < EntityStorage.oldestTime){
//      throw PushedOutOfGraphException(time)
//    }

//        for((key,prop) <- properties){
//          if(!prop.equals(v2.properties.getOrElse(key,null))){
//            return false
//          }
//        }
