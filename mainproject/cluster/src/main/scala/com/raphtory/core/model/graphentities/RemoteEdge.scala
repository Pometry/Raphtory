package com.raphtory.core.model.graphentities

import com.raphtory.core.storage.EntityStorage
import com.raphtory.core.utils.Utils

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap

/**
  * Companion Edge object (extended creator for storage loads)
  */
object RemoteEdge {
  def apply(workerID:Int,creationTime : Long, srcID:Long,dstID:Long, previousState : mutable.TreeMap[Long, Boolean], properties : ParTrieMap[String, Property], remotePartitionId : Int,storage:EntityStorage)= {
    val e = new RemoteEdge(workerID:Int,creationTime, srcID, dstID, initialValue = true, remotePartitionId,storage)
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
  */
class RemoteEdge(workerID:Int, msgTime: Long, srcID: Long, dstID: Long, initialValue: Boolean, remotePartitionId: Int, storage:EntityStorage) extends Edge(workerID,msgTime, srcID, dstID, initialValue,storage){
  def remotePartitionID =remotePartitionId
}

