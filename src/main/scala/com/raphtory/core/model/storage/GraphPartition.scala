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

   var managerCount: Int = initManagerCount
  //stuff for compression and archiving
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

  def addVertex(msgTime: Long, srcId: Long, properties: Properties, vertexType: Option[Type]): RaphtoryVertex

  def vertexWorkerRequest(msgTime: Long, srcId: Long, dstId: Long, edge: RaphtoryEdge, present: Boolean, channelId: String, channelTime: Int): TrackedGraphEffect[GraphUpdateEffect]

  def vertexWipeWorkerRequest(msgTime: Long, srcId: Long, dstId: Long, edge: RaphtoryEdge, present: Boolean, channelId: String, channelTime: Int): TrackedGraphEffect[GraphUpdateEffect]

  def vertexWorkerRequestEdgeHandler(msgTime: Long, srcId: Long, dstId: Long, removeList: List[(Long, Boolean)]): Unit

  def removeVertex(msgTime: Long, srcId: Long, channelId: String, channelTime: Int): List[TrackedGraphEffect[GraphUpdateEffect]]

  /**
    * Edges Methods
    */
  def addEdge(msgTime: Long, srcId: Long, dstId: Long, channelId: String, channelTime: Int, properties: Properties, edgeType: Option[Type]): Option[TrackedGraphEffect[GraphUpdateEffect]]

  def remoteEdgeAddNew(msgTime: Long, srcId: Long, dstId: Long, properties: Properties, srcDeaths: List[(Long, Boolean)], edgeType: Option[Type], channelId: String, channelTime: Int): TrackedGraphEffect[GraphUpdateEffect]

  def remoteEdgeAdd(msgTime: Long, srcId: Long, dstId: Long, properties: Properties, channelId: String, channelTime: Int): TrackedGraphEffect[GraphUpdateEffect]

  def removeEdge(msgTime: Long, srcId: Long, dstId: Long, channelId: String, channelTime: Int): Option[TrackedGraphEffect[GraphUpdateEffect]]

  def returnEdgeRemoval(msgTime: Long, srcId: Long, dstId: Long, channelId: String, channelTime: Int): TrackedGraphEffect[GraphUpdateEffect]

  def edgeRemovalFromOtherWorker(msgTime: Long, srcId: Long, dstId: Long, channelId: String, channelTime: Int): TrackedGraphEffect[GraphUpdateEffect]

  def remoteEdgeRemoval(msgTime: Long, srcId: Long, dstId: Long, channelId: String, channelTime: Int): TrackedGraphEffect[GraphUpdateEffect]

  def remoteEdgeRemovalFromVertex(msgTime: Long, srcId: Long, dstId: Long, channelId: String, channelTime: Int): TrackedGraphEffect[GraphUpdateEffect]

  def remoteEdgeRemovalNew(msgTime: Long, srcId: Long, dstId: Long, srcDeaths: List[(Long, Boolean)], channelId: String, channelTime: Int): TrackedGraphEffect[GraphUpdateEffect]

  def remoteReturnDeaths(msgTime: Long, srcId: Long, dstId: Long, dstDeaths: List[(Long, Boolean)]): Unit

  //TODO these are placed here until YanYangs changes can be integrated
  def getManager(srcId: Long, managerCount: Int): String = {
    val mod = srcId.abs % (managerCount * 10)
    val manager = mod / 10
    val worker = mod % 10
    s"/user/Manager_${manager}_child_$worker"
  }

  def checkDst(dstID: Long, managerCount: Int, managerID: Int): Boolean =
    ((dstID.abs % (managerCount * 10)) / 10).toInt == managerID //check if destination is also local

  def checkWorker(dstID: Long, managerCount: Int, workerID: Int): Boolean =
    ((dstID.abs % (managerCount * 10)) % 10).toInt == workerID //check if destination is also local
}
