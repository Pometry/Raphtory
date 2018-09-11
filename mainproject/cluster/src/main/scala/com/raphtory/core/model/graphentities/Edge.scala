package com.raphtory.core.model.graphentities

import com.raphtory.core.utils.Utils

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
//        for((key,prop) <- properties){
//          if(!prop.equals(v2.properties.getOrElse(key,null))){
//            return false
//          }
//        }
        return true
      }
    }
    false
  }

  override def toString: String = {
    s"Edge srcID $srcId dstID $dstId \n History $previousState \n Properties:\n $properties"
  }

}
