package com.raphtory.core.model.graphentities

import com.raphtory.core.utils.Utils

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap

/**
  * Companion Edge object (extended creator for storage loads)
  */
object RemoteEdge {
  def apply(routerID : Int, workerID:Int,creationTime : Long, edgeId : Long,
            previousState : mutable.TreeMap[Long, Boolean],
            properties : ParTrieMap[String, Property], remotePos : RemotePos.Value, remotePartitionId : Int)= {

    val srcId = Utils.getIndexHI(edgeId)
    val dstId = Utils.getIndexLO(edgeId)

    val e = new RemoteEdge(routerID,workerID:Int,creationTime, srcId, dstId, initialValue = true, remotePos, remotePartitionId)
    e.previousState   = previousState
    e.properties      = properties
    e
  }
}
/** *
  * Extension of the Edge entity, used when we want to store a remote edge
  * i.e. one spread across two partitions
  * currently only stores what end of the edge is remote
  * and which partition this other half is stored in
  *
  * @param msgTime
  * @param initialValue
  * @param srcId
  * @param dstId
  * @param remotePos
  * @param remotePartitionID
  */
class RemoteEdge(routerID: Int,
                 workerID:Int,
                      msgTime: Long,
                      srcID: Int,
                      dstID: Int,
                      initialValue: Boolean,
                      remotepos: RemotePos.Value,
                      remotePartitionId: Int)
    extends Edge(routerID,workerID,msgTime, srcID, dstID, initialValue){

  def remotePos = remotepos
  def remotePartitionID =remotePartitionId
  def srcId = srcID
  def dstId = dstID

  override def equals(obj: scala.Any): Boolean = {
    if(obj.isInstanceOf[Edge]){
      val v2 = obj.asInstanceOf[Edge] //add associated edges
      if((getSrcId == v2.getSrcId) && (getDstId == v2.getDstId) && (previousState.equals(v2.previousState)) && (oldestPoint.get == v2.oldestPoint.get) && (newestPoint.get == newestPoint.get) && (properties.equals(v2.properties.size))){
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
}

object RemotePos extends Enumeration {
  type RemoteLocation = Value
  val Source, Destination = Value
}

