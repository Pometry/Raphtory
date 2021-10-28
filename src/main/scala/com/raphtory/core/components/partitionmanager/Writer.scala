package com.raphtory.core.components.partitionmanager

import akka.actor.{ActorRef, Cancellable}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.components.graphbuilder.BuilderExecutor.Message.BuilderTimeSync
import com.raphtory.core.components.partitionmanager.Writer.Message.Watermark
import com.raphtory.core.components.akkamanagement.RaphtoryActor._
import com.raphtory.core.components.akkamanagement.{MailboxTrackedActor, RaphtoryActor}
import com.raphtory.core.components.leader.WatermarkManager.Message.{ProbeWatermark, WatermarkTime}
import com.raphtory.core.implementations.pojograph.messaging._
import com.raphtory.core.model.graph.{EdgeAdd, EdgeDelete, EdgeSyncAck, GraphPartition, GraphUpdateEffect, InboundEdgeRemovalViaVertex, OutboundEdgeRemovalViaVertex, SyncExistingEdgeAdd, SyncExistingEdgeRemoval, SyncExistingRemovals, SyncNewEdgeAdd, SyncNewEdgeRemoval, TrackedGraphEffect, TrackedGraphUpdate, VertexAdd, VertexDelete, VertexRemoveSyncAck}

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._


final class Writer(partitionID:Int, storage: GraphPartition) extends RaphtoryActor with MailboxTrackedActor {

  private var increments = 0
  private var updates = 0
  private var updates2 = 0
  private var vertexAdds = 0

  private val queuedMessageMap = mutable.Map[String, mutable.PriorityQueue[queueItem]]()
  private val safeMessageMap = mutable.Map[String, queueItem]()
  private val vDeleteCountdownMap = mutable.Map[(String, Int), AtomicInteger]()

  override def receive: Receive = mailboxTrackedReceive {
    case TrackedGraphUpdate(channelId, channelTime, req: VertexAdd) => processVertexAddRequest(channelId, channelTime, req) //Add a new vertex
    case TrackedGraphUpdate(channelId, channelTime, req: EdgeAdd) => processEdgeAddRequest(channelId, channelTime, req) //Add an edge

    case TrackedGraphEffect(channelId, channelTime, req: SyncNewEdgeAdd) => processSyncNewEdgeAdd(channelId, channelTime, req) //A writer has requested a new edge sync for a destination node in this worker
    case TrackedGraphEffect(channelId, channelTime, req: SyncExistingEdgeAdd) => processSyncExistingEdgeAdd(channelId, channelTime, req) // A writer has requested an existing edge sync for a destination node on in this worker
    case TrackedGraphEffect(channelId, channelTime, req: SyncExistingRemovals) => processSyncExistingRemovals(channelId, channelTime, req) //The remote worker has returned all removals in the destination node -- for new edges
    case TrackedGraphEffect(channelId, channelTime, req: EdgeSyncAck) => processEdgeSyncAck(channelId, channelTime, req) //The remote worker acknowledges the completion of an edge sync

    case TrackedGraphUpdate(channelId, channelTime, req: EdgeDelete) => processEdgeDelete(channelId, channelTime, req) //Delete an Edge
    case TrackedGraphEffect(channelId, channelTime, req: SyncNewEdgeRemoval) => processSyncNewEdgeRemoval(channelId, channelTime, req) //A remote worker is asking for a new edge to be removed for a destination node in this worker
    case TrackedGraphEffect(channelId, channelTime, req: SyncExistingEdgeRemoval) => processSyncExistingEdgeRemoval(channelId, channelTime, req) //A remote worker is asking for the deletion of an existing edge

    case TrackedGraphUpdate(channelId, channelTime, req: VertexDelete) => processVertexDelete(channelId, channelTime, req) //Delete a vertex and all associated edges
    case TrackedGraphEffect(channelId, channelTime, req: OutboundEdgeRemovalViaVertex) => processOutboundEdgeRemovalViaVertex(channelId, channelTime, req) //Does exactly the same as above, but for when the removal comes form a vertex
    case TrackedGraphEffect(channelId, channelTime, req: InboundEdgeRemovalViaVertex) => processInboundEdgeRemovalViaVertex(channelId, channelTime, req) // Excatly the same as above, but for a remote worker

    case TrackedGraphEffect(channelId, channelTime, req: VertexRemoveSyncAck) => processVertexRemoveSyncAck(channelId, channelTime, req)

    case Watermark => processWatermarkRequest(); //println(s"$workerId ${storage.newestTime} ${storage.windowTime} ${storage.newestTime-storage.windowTime}")
    case ProbeWatermark => mediator ! DistributedPubSubMediator.Send("/user/WatermarkManager", WatermarkTime(storage.windowTime), localAffinity = false)
    case req: BuilderTimeSync => processBuilderTimeSync(req);
    //case SaveState => serialiseGraphPartition();
    case x => log.warning(s"IngestionWorker [{}] received unknown [{}] message.", partitionID, x)
  }


  def processVertexAddRequest(channelId: String, channelTime: Int, update: VertexAdd): Unit = {
    log.debug(s"IngestionWorker [$partitionID] received [$update] request.")
    storage.addVertex(update.updateTime, update.srcId, update.properties, update.vType)

    trackVertexAdd(update.updateTime, channelId, channelTime)
  }

  private def trackVertexAdd(msgTime: Long, channelId: String, channelTime: Int): Unit = {
    //Vertex Adds the message time straight into queue as no sync
    storage.timings(msgTime)
    vertexAdds+=1
    addToWatermarkQueue(channelId, channelTime, msgTime)
  }


  def processEdgeAddRequest(channelId: String, channelTime: Int, update: EdgeAdd): Unit = {
    log.debug(s"IngestionWorker [$partitionID] received [$update] request.")
    val maybeEffect = storage.addEdge(update.updateTime, update.srcId, update.dstId, update.properties, update.eType, channelId, channelTime)
    maybeEffect.foreach(sendEffectMessage)
    trackEdgeAdd(update.updateTime, maybeEffect.isEmpty, channelId, channelTime)
  }

  private def trackEdgeAdd(msgTime: Long, local: Boolean, channelId: String, channelTime: Int): Unit = {
    storage.timings(msgTime)
    if (local) { //if the edge is totally handled by this worker then we are safe to add to watermark queue
      addToWatermarkQueue(channelId, channelTime, msgTime)
    }
  }

  def processSyncExistingEdgeAdd(channelId: String, channelTime: Int, req: SyncExistingEdgeAdd): Unit = {
    log.debug(s"IngestionWorker [$partitionID] received [$req] request.")
    val effect = storage.syncExistingEdgeAdd(req.msgTime, req.srcId, req.dstId, req.properties, channelId, channelTime)
    sendEffectMessage(effect)
    remoteEdgeAddTrack(req.msgTime)
  }

  def processSyncNewEdgeAdd(channelId: String, channelTime: Int, req: SyncNewEdgeAdd): Unit = {
    log.debug(s"IngestionWorker [$partitionID] received [$req] request.")
    val effect = storage.syncNewEdgeAdd(req.msgTime, req.srcId, req.dstId, req.properties, req.removals, req.vType, channelId, channelTime)
    sendEffectMessage(effect)
    remoteEdgeAddTrack(req.msgTime)
  }

  private def remoteEdgeAddTrack(msgTime: Long): Unit = {
    storage.timings(msgTime)
  }

  //
  def processSyncExistingRemovals(channelId: String, channelTime: Int, req: SyncExistingRemovals): Unit = { //when the new edge add is responded to we can say it is synced
    log.debug(s"IngestionWorker [$partitionID] received [$req] request.")
    storage.syncExistingRemovals(req.msgTime, req.srcId, req.dstId, req.removals)
    addToWatermarkQueue(channelId, channelTime, req.msgTime)
  }

  def processEdgeSyncAck(channelId: String, channelTime: Int, req: EdgeSyncAck) = { //when the edge isn't new we will get this response instead
    log.debug(s"IngestionWorker [$partitionID] received [$req] request.")
    addToWatermarkQueue(channelId, channelTime, req.msgTime)
  }


  def processEdgeDelete(channelId: String, channelTime: Int, update: EdgeDelete): Unit = {
    log.debug(s"IngestionWorker [$partitionID] received [$update] request.")
    val maybeEffect = storage.removeEdge(update.updateTime, update.srcId, update.dstId, channelId, channelTime)
    maybeEffect.foreach(sendEffectMessage)
    trackEdgeDelete(update.updateTime, maybeEffect.isEmpty, channelId, channelTime)
  }

  private def trackEdgeDelete(msgTime: Long, local: Boolean, channelId: String, channelTime: Int): Unit = {
    storage.timings(msgTime)
    if (local) { //if the edge is totally handled by this worker then we are safe to add to watermark queue
      addToWatermarkQueue(channelId, channelTime, msgTime)
    }
  }

  def processSyncNewEdgeRemoval(channelId: String, channelTime: Int, req: SyncNewEdgeRemoval): Unit = {
    log.debug(s"IngestionWorker [$partitionID] received [$req] request.")
    val effect = storage.syncNewEdgeRemoval(req.msgTime, req.srcId, req.dstId, req.removals, channelId, channelTime)
    sendEffectMessage(effect)
    storage.timings(req.msgTime)
  }

  def processSyncExistingEdgeRemoval(channelId: String, channelTime: Int, req: SyncExistingEdgeRemoval): Unit = {
    log.debug(s"IngestionWorker [$partitionID] received [$req] request.")
    val effect = storage.syncExistingEdgeRemoval(req.msgTime, req.srcId, req.dstId, channelId, channelTime)
    sendEffectMessage(effect)
    storage.timings(req.msgTime)
  }


  def processVertexDelete(channelId: String, channelTime: Int, update: VertexDelete): Unit = {
    log.debug(s"IngestionWorker [$partitionID] received [$update] request.")
    val messages = storage.removeVertex(update.updateTime, update.srcId, channelId, channelTime)
    messages.foreach(x => sendEffectMessage(x))
    trackVertexDelete(update.updateTime, channelId, channelTime, messages.size)
  }

  private def trackVertexDelete(msgTime: Long, channelId: String, channelTime: Int, totalCount: Int): Unit = {
    if (totalCount == 0) //if there are no outgoing edges it is safe to watermark
      addToWatermarkQueue(channelId, channelTime, msgTime)
    else {
      vDeleteCountdownMap put((channelId, channelTime), new AtomicInteger(totalCount))
    }
    storage.timings(msgTime)
  }

  def processVertexRemoveSyncAck(channelId: String, channelTime: Int, req: VertexRemoveSyncAck) = {
    vDeleteCountdownMap.get((channelId, channelTime)) match {
      case Some(integer) => if (integer.decrementAndGet() == 0) {
        addToWatermarkQueue(channelId, channelTime, req.msgTime)
        vDeleteCountdownMap.remove((channelId, channelTime)) //todo improve this datastructure
      }
      case None => log.error(s"$req does not match records in vDeleteCountdownMap")
    }
  }

  def processOutboundEdgeRemovalViaVertex(channelId: String, channelTime: Int, req: OutboundEdgeRemovalViaVertex): Unit = {
    log.debug(s"IngestionWorker [$partitionID] received [$req] request.")
    val effect = storage.outboundEdgeRemovalViaVertex(req.msgTime, req.srcId, req.dstId, channelId, channelTime)
    sendEffectMessage(effect)
    storage.timings(req.msgTime)
  }

  def processInboundEdgeRemovalViaVertex(channelId: String, channelTime: Int, req: InboundEdgeRemovalViaVertex): Unit = { //remote worker same as above
    log.debug(s"IngestionWorker [$partitionID] received [$req] request.")
    val effect = storage.inboundEdgeRemovalViaVertex(req.msgTime, req.srcId, req.dstId, channelId, channelTime)
    sendEffectMessage(effect)
  }

  private def addToWatermarkQueue(channelId: String, channelTime: Int, msgTime: Long) = {
    queuedMessageMap.get(channelId) match {
      case Some(queue) => queue += queueItem(channelTime, msgTime)
      case None =>
        val queue = new mutable.PriorityQueue[queueItem]()(Ordering.by[queueItem, Int](f => f.builderEpoch).reverse)
        queue += queueItem(channelTime, msgTime)
        queuedMessageMap put(channelId, queue)
    }
    updates += 1
  }


  private def processWatermarkRequest() = {
    if (queuedMessageMap nonEmpty) {
      val queueState = queuedMessageMap.map(queue => {
        setSafePoint(queue._1, queue._2)
      })
      val timestamps = queueState.map(q => q.timestamp)
      //// println(s"$increments Writer Worker $partitionID $workerId $timestamps")
      //        println(s"Writer Worker $partitionID $workerId ${queueState.mkString("[",",","]")} ${storage.vertices.size}")
      //println(s"$increments Writer Worker $partitionID $workerId ${timestamps.min} ${storage.vertices.size} $updates ${updates-updates2}")
      updates2 = updates
      increments += 1

      val min = timestamps.min
      if (storage.windowTime < min)
        storage.windowTime = min
    }
  }

  import scala.util.control.Breaks._

  private def setSafePoint(builderName: String, messageQueue: mutable.PriorityQueue[queueItem]) = {

    var currentSafePoint = safeMessageMap.get(builderName) match {
      case Some(value) => value
      case None => queueItem(-1, 0)
    }
    breakable {
      while (messageQueue nonEmpty)
        if (messageQueue.head.builderEpoch == currentSafePoint.builderEpoch + 1)
          currentSafePoint = messageQueue.dequeue()
        else if (messageQueue.head.builderEpoch == currentSafePoint.builderEpoch)
          currentSafePoint = messageQueue.dequeue()
        else {
          break
        }
    }
    safeMessageMap put(builderName, currentSafePoint)
    currentSafePoint
  }

  private def processBuilderTimeSync(req: BuilderTimeSync) = {
    storage.timings(req.msgTime)
    addToWatermarkQueue(req.BuilderId, req.builderTime, req.msgTime)
  }

  private def sendEffectMessage[T <: GraphUpdateEffect](msg: TrackedGraphEffect[T]): Unit =
    mediator ! new DistributedPubSubMediator.Send(getWriter(msg.effect.updateId), msg)


  private val scheduledTaskMap: mutable.HashMap[String, Cancellable] = mutable.HashMap[String, Cancellable]()

  override def preStart() {
    log.debug("IngestionWorker is being started.")
    scheduleTasks()
  }

  override def postStop() {
    val allTasksCancelled = scheduledTaskMap.forall {
      case (key, task) =>
        cancelTask(key, task)
    }
    if (!allTasksCancelled) log.warning("Failed to cancel all scheduled tasks post stop.")
  }

  private def scheduleTasks(): Unit = {
    log.debug("Preparing to schedule tasks in Spout.")
    val watermarkCancellable =
      scheduleTask(initialDelay = 10 seconds, interval = 5 second, receiver = self, message = Watermark)
    scheduledTaskMap.put("watermark", watermarkCancellable)
  }

  case class queueItem(builderEpoch:Int, timestamp:Long)extends Ordered[queueItem] {
    def compare(that: queueItem): Int = that.builderEpoch-this.builderEpoch
  }
}

object Writer {
  object Message {
    case object Watermark
  }
}
//  def serialiseGraphPartition() = {
//    ParquetWriter.writeAndClose(s"/Users/Mirate/github/test/$partitionID/$workerId/graph.parquet", storage.vertices.map(x=>x._2.serialise()).toArray.toIterable)
//    ParquetWriter.writeAndClose(s"/Users/Mirate/github/test/$partitionID/$workerId/state.parquet", List(StorageState(storage.managerCount,storage.oldestTime,storage.newestTime,storage.windowTime)).toIterable)
//  }
//  def deserialiseGraphPartitions() = {
//    val graph = ParquetReader.read[ParquetVertex](s"/Users/Mirate/github/test/$partitionID/$workerId/graph.parquet")
//    try {
//      graph.foreach(vertex => storage.vertices+=((vertex.id,RaphtoryVertex(vertex))))
//    } finally graph.close()
//
//    val state = ParquetReader.read[StorageState](s"/Users/Mirate/github/test/$partitionID/$workerId/graph.parquet")
//    try {
//      state.foreach(stats => {
//        storage.managerCount=stats.managerCount
//        storage.oldestTime=stats.oldestTime
//        storage.newestTime=stats.newestTime
//        storage.windowTime=stats.windowTime
//      })
//    } finally state.close()
//  }

//case class ParquetProperty(key:String,immutable:Boolean,history:List[(Long,String)])
//case class ParquetEdge(src:Long, dst:Long, split:Boolean, history:List[(Long,Boolean)], properties:List[ParquetProperty])
//case class ParquetVertex(id:Long, history:List[(Long,Boolean)], properties:List[ParquetProperty], incoming:List[ParquetEdge], outgoing:List[ParquetEdge])
//case class StorageState(managerCount:Int,oldestTime: Long,newestTime: Long,windowTime: Long )


