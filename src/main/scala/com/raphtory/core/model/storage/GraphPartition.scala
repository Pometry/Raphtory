package com.raphtory.core.model.storage

import com.raphtory.core.model.communication._
import com.raphtory.core.model.implementations.entities.{RaphtoryEdge, RaphtoryEntity, RaphtoryVertex, SplitRaphtoryEdge}
import com.typesafe.scalalogging.LazyLogging
import kamon.Kamon

import scala.collection.parallel.mutable.ParTrieMap

/**
  * Singleton representing the Storage for the entities
  */

abstract class GraphPartition(initManagerCount: Int, partitionID: Int, workerID: Int) extends LazyLogging {

  /**
    * Ingesting Vertices
    */

  def addVertex(msgTime: Long, srcId: Long, properties: Properties, vertexType: Option[Type]): Unit

  def removeVertex(msgTime: Long, srcId: Long, channelId: String, channelTime: Int): List[TrackedGraphEffect[GraphUpdateEffect]]
  def inboundEdgeRemovalViaVertex(msgTime: Long, srcId: Long, dstId: Long, channelId: String, channelTime: Int): TrackedGraphEffect[GraphUpdateEffect]
  def outboundEdgeRemovalViaVertex(msgTime: Long, srcId: Long, dstId: Long, channelId: String, channelTime: Int): TrackedGraphEffect[GraphUpdateEffect]

  /**
    * Ingesting Edges
    * */

  def addEdge(msgTime: Long, srcId: Long, dstId: Long, properties: Properties, edgeType: Option[Type], channelId: String, channelTime: Int): Option[TrackedGraphEffect[GraphUpdateEffect]]
  def syncNewEdgeAdd(msgTime: Long, srcId: Long, dstId: Long, properties: Properties, srcRemovals: List[Long], edgeType: Option[Type], channelId: String, channelTime: Int): TrackedGraphEffect[GraphUpdateEffect]
  def syncExistingEdgeAdd(msgTime: Long, srcId: Long, dstId: Long, properties: Properties, channelId: String, channelTime: Int): TrackedGraphEffect[GraphUpdateEffect]

  def removeEdge(msgTime: Long, srcId: Long, dstId: Long, channelId: String, channelTime: Int): Option[TrackedGraphEffect[GraphUpdateEffect]]
  def syncNewEdgeRemoval(msgTime: Long, srcId: Long, dstId: Long, srcRemovals: List[Long], channelId: String, channelTime: Int): TrackedGraphEffect[GraphUpdateEffect]
  def syncExistingEdgeRemoval(msgTime: Long, srcId: Long, dstId: Long, channelId: String, channelTime: Int): TrackedGraphEffect[GraphUpdateEffect]

  def syncExistingRemovals(msgTime: Long, srcId: Long, dstId: Long, dstRemovals: List[Long]): Unit


  def getVertices():ParTrieMap[Long,RaphtoryVertex]



  var managerCount: Int = initManagerCount
  var oldestTime: Long = Long.MaxValue
  var newestTime: Long = 0
  var windowTime: Long = 0

  val vertexCount =
    Kamon.counter("Raphtory_Vertex_Count").withTag("actor", s"PartitionWriter_$partitionID").withTag("ID", workerID)
  val localEdgeCount =
    Kamon.counter("Raphtory_Local_Edge_Count").withTag("actor", s"PartitionWriter_$partitionID").withTag("ID", workerID)
  val copySplitEdgeCount = Kamon
    .counter("Raphtory_Copy_Split_Edge_Count")
    .withTag("actor", s"PartitionWriter_$partitionID")
    .withTag("ID", workerID)
  val masterSplitEdgeCount = Kamon
    .counter("Raphtory_Master_Split_Edge_Count")
    .withTag("actor", s"PartitionWriter_$partitionID")
    .withTag("ID", workerID)

  def timings(updateTime: Long) = {
    if (updateTime < oldestTime && updateTime > 0) oldestTime = updateTime
    if (updateTime > newestTime)
      newestTime = updateTime //this isn't thread safe, but is only an approx for the archiving
  }

  def setManagerCount(count: Int) = this.managerCount = count

  def getPartitionID = partitionID
  def getWorkerID = workerID



   def checkDst(dstID: Long, managerCount: Int, managerID: Int,workerID:Int): Boolean =
     (((dstID.abs % (managerCount * 10)) / 10).toInt == managerID) &&
       (((dstID.abs % (managerCount * 10)) % 10).toInt == workerID) //check if destination is also local)

}
