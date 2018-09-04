package com.raphtory.core.model.graphentities

import com.raphtory.core.utils.Utils

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap

/**
  * Companion Edge object (extended creator for storage loads)
  */
object RemoteEdge {
  def apply(routerID : Int, creationTime : Long, edgeId : Long,
            previousState : mutable.TreeMap[Long, Boolean],
            properties : ParTrieMap[String, Property], remotePos : RemotePos.Value, remotePartitionId : Int)= {

    val srcId = Utils.getIndexHI(edgeId)
    val dstId = Utils.getIndexLO(edgeId)

    val e = new RemoteEdge(routerID,creationTime, srcId, dstId, initialValue = true, addOnly = false, remotePos, remotePartitionId)
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
case class RemoteEdge(routerID: Int,
                      msgTime: Long,
                      srcId: Int,
                      dstId: Int,
                      initialValue: Boolean,
                      addOnly:Boolean,
                      remotePos: RemotePos.Value,
                      remotePartitionID: Int)
    extends Edge(routerID,msgTime, srcId, dstId, initialValue, addOnly)
