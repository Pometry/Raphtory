package com.raphtory.core.model.graphentities

import com.raphtory.core.actors.partitionmanager.Archivist.{EdgeHistoryPoint, EdgePropertyPoint, VertexPropertyPoint}
import com.raphtory.core.utils.Utils
import com.raphtory.core.utils.exceptions.EntityRemovedAtTimeException

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap
/**
  * Companion Edge object (extended creator for storage loads)
  */
object Edge {
  def apply(routerID:Int,creationTime : Long, edgeId : Long,
            previousState : mutable.TreeMap[Long, Boolean],
            properties : ParTrieMap[String, Property]) = {

    val srcId = Utils.getIndexHI(edgeId)
    val dstId = Utils.getIndexLO(edgeId)

    val e = new Edge(routerID,creationTime, srcId, dstId, initialValue = true, addOnly = false)
    e.previousState   = previousState
    e.properties      = properties
    e
  }

  def apply(saved:EdgeHistoryPoint, time:Long) = {
    val src = saved.src
    val dst = saved.dst
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
    if(!value){
      throw EntityRemovedAtTimeException(Utils.getEdgeIndex(src,dst))
    }
    new Edge(-1,closestTime,src,dst,value,false)
  }
}

/**
  * Created by Mirate on 01/03/2017.
  */
class Edge(routerID:Int, msgTime: Long, srcId: Int, dstId: Int, initialValue: Boolean, addOnly:Boolean)
    extends Entity(routerID,msgTime, initialValue,addOnly) {

  /*override def printProperties: String =
    s"Edge between $srcId and $dstId: with properties: " + System
      .lineSeparator() +
      super.printProperties()
  */

  def killList(vKills: mutable.TreeMap[Long, Boolean]): Unit = {
    removeList ++= vKills
    if (!addOnly)
      previousState ++= vKills
  }

  override def getId: Long = Utils.getEdgeIndex(srcId, dstId)
  def getSrcId : Int = srcId
  def getDstId : Int = dstId

  override def equals(obj: scala.Any): Boolean = {
    if(obj.isInstanceOf[Edge]){
      val v2 = obj.asInstanceOf[Edge] //add associated edges
      if((getSrcId == v2.getSrcId) && (getDstId == v2.getDstId) && (previousState.equals(v2.previousState)) && (oldestPoint.get == v2.oldestPoint.get) && (newestPoint.get == newestPoint.get) && (properties.equals(v2.properties))){
        return true
      }
    }
    false
  }

  def addSavedProperty(property:EdgePropertyPoint, time:Long): Unit ={
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

  override def toString: String = {
    s"Edge srcID $srcId dstID $dstId \n History $previousState \n Properties:\n $properties"
  }

}
