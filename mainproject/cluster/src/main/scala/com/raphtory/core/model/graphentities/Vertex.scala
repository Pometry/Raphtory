package com.raphtory.core.model.graphentities

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


  def aliveAt(time:Long):Boolean = {
    if(time < EntityStorage.oldestTime)
      false
    var closestTime:Long = 0
    var value = false
    for((k,v) <- previousState){
      if(k<=time)
        if((time-k)<(time-closestTime)) {
          closestTime = k
          value = v
        }
    }
    value
  }

  def viewAt(time:Long):Vertex = {
    //    if(time > EntityStorage.lastCompressedAt){
    //      throw StillWithinLiveGraphException(time)
    //    }
    if(time < EntityStorage.oldestTime){
      throw PushedOutOfGraphException(time)
    }
    var closestTime:Long = 0
    var value = false
    for((k,v) <- previousState){
      if(k<=time)
        if((time-k)<(time-closestTime)) {
          closestTime = k
          value = v
        }
    }
    if(!value)
      throw EntityRemovedAtTimeException(vertexId)


    val vertex = new Vertex(-1,closestTime,vertexId,value)
    vertex.setCompMap(this.computationValues)
    vertex.setmutliQueue(this.vertexMultiQueue)
    for((k,p) <- properties) {
      val value = p.valueAt(time)
      if (!(value equals("default")))
        vertex  + (time,k,value)
    }
    for(e <- incomingIDs.toArray){
      if(EntityStorage.edges(Utils.getEdgeIndex(e,vertexId)).aliveAt(time))
        vertex.addIncomingEdge(e)
    }
    for(e <- outgoingIDs.toArray){
      if(EntityStorage.edges(Utils.getEdgeIndex(vertexId,e)).aliveAt(time))
        vertex.addIncomingEdge(e)
    }
    vertex
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
        println(s"vertex id $vertexId")
        println("properties incorrect:")
        println(properties)
        println(v2.properties)
        false
      }

      else if(!(incomingEdges.equals(v2.incomingEdges))){
        println("associated edges incorrect:")
        println(incomingEdges)
        println(v2.incomingEdges)
        false
      }
      else if(!(outgoingEdges.equals(v2.outgoingEdges))){
        println("associated edges incorrect:")
        println(outgoingEdges)
        println(v2.outgoingEdges)
        false
      }
      else true
    }
    else false

  }

  override def toString: String = {
    //    s"Vertex ID $vertexId \n History $previousState \n Properties:\n $properties \n Associated Edges: $associatedEdges"
    s"Vertex ID $vertexId \n History $previousState \n //Properties:\n $properties \n"
  }

}

//        for((key,prop) <- properties){
//          if(!prop.equals(v2.properties.getOrElse(key,null))){
//            return false
//          }
//        }
