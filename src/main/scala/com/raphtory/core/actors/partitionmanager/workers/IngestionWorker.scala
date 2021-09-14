package com.raphtory.core.actors.partitionmanager.workers

import akka.actor.{ActorRef, Cancellable}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.actors.partitionmanager.workers.IngestionWorker.Message.Watermark
import com.raphtory.core.actors.{MailboxTrackedActor, RaphtoryActor}
import com.raphtory.core.actors.graphbuilder.RouterWorker.CommonMessage.RouterWorkerTimeSync
import com.raphtory.core.model.communication._
import kamon.Kamon

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import com.github.mjakubowski84.parquet4s.{ParquetReader, ParquetWriter}
import com.raphtory.core.actors.orchestration.clustermanager.WatermarkManager.Message.{ProbeWatermark, SaveState, WatermarkTime}
import com.raphtory.core.model.implementations.entities.RaphtoryVertex
import com.raphtory.core.model.storage.GraphPartition



case class queueItem(routerEpoch:Int,timestamp:Long)extends Ordered[queueItem] {
  def compare(that: queueItem): Int = that.routerEpoch-this.routerEpoch
}

// TODO Re-add compression (removed during commit Log-Revamp)
final class IngestionWorker(workerId: Int, partitionID:Int, storage: GraphPartition, managerCount:Int) extends RaphtoryActor with MailboxTrackedActor {
  private implicit val executionContext: ExecutionContext = context.system.dispatcher

  private val mediator: ActorRef = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)

  private var increments =0
  private var updates = 0
  private var updates2 =0

  private val routerUpdates       = Kamon.counter("Raphtory_Router_Updates").withTag("actor",s"PartitionWriter_$partitionID").withTag("ID",workerId)
  private val interWorkerUpdates  = Kamon.counter("Raphtory_Inter_Worker_Updates").withTag("actor",s"PartitionWriter_$partitionID").withTag("ID",workerId)
  private val intraWorkerUpdates  = Kamon.counter("Raphtory_Intra_Worker_Updates").withTag("actor",s"PartitionWriter_$partitionID").withTag("ID",workerId)
  private val synchronisedUpdates = Kamon.counter("Raphtory_Synchronised_Updates").withTag("actor",s"PartitionWriter_$partitionID").withTag("ID",workerId)

  private val safeTime            = Kamon.gauge("Raphtory_Safe_Time").withTag("actor",s"PartitionWriter_$partitionID").withTag("ID",workerId)
  private val latestTime          = Kamon.gauge("Raphtory_Latest_Time").withTag("actor",s"PartitionWriter_$partitionID").withTag("ID",workerId)
  private val earliestTime        = Kamon.gauge("Raphtory_Earliest_Time").withTag("actor",s"PartitionWriter_$partitionID").withTag("ID",workerId)


  private val queuedMessageMap = ParTrieMap[String, mutable.PriorityQueue[queueItem]]()
  private val safeMessageMap = ParTrieMap[String, queueItem]()
  private val vDeleteCountdownMap = ParTrieMap[(String,Int), AtomicInteger]()

  override def receive: Receive = mailboxTrackedReceive {
    case TrackedGraphUpdate(channelId, channelTime, req: VertexAdd) => processVertexAddRequest(channelId, channelTime, req); //Add a new vertex
    case TrackedGraphUpdate(channelId, channelTime, req: EdgeAdd) => processEdgeAddRequest(channelId, channelTime, req) //Add an edge

    case TrackedGraphEffect(channelId, channelTime, req: SyncNewEdgeAdd) => processRemoteEdgeAddNewRequest(channelId, channelTime, req) //A writer has requested a new edge sync for a destination node in this worker
    case TrackedGraphEffect(channelId, channelTime, req: SyncExistingEdgeAdd) => processRemoteEdgeAddRequest(channelId, channelTime, req) // A writer has requested an existing edge sync for a destination node on in this worker
    case TrackedGraphEffect(channelId, channelTime, req: SyncExistingRemovals) => processRemoteReturnDeathsRequest(channelId, channelTime, req) //The remote worker has returned all removals in the destination node -- for new edges
    case TrackedGraphEffect(channelId, channelTime, req: EdgeSyncAck) => processEdgeSyncAck(channelId, channelTime, req) //The remote worker acknowledges the completion of an edge sync

    case TrackedGraphUpdate(channelId, channelTime, req: EdgeDelete) => processEdgeDeleteRequest(channelId, channelTime, req) //Delete an Edge
    case TrackedGraphEffect(channelId, channelTime, req: SyncNewEdgeRemoval) => processRemoteEdgeRemovalNewRequest(channelId, channelTime, req) //A remote worker is asking for a new edge to be removed for a destination node in this worker
    case TrackedGraphEffect(channelId, channelTime, req: SyncExistingEdgeRemoval) => processRemoteEdgeRemovalRequest(channelId, channelTime, req) //A remote worker is asking for the deletion of an existing edge

    case TrackedGraphUpdate(channelId, channelTime, req: VertexDelete) => processVertexDeleteRequest(channelId, channelTime, req) //Delete a vertex and all associated edges
    case TrackedGraphEffect(channelId, channelTime, req: OutboundEdgeRemovalViaVertex) => processRemoteEdgeRemovalRequestFromVertex(channelId, channelTime, req) //Does exactly the same as above, but for when the removal comes form a vertex
    case TrackedGraphEffect(channelId, channelTime, req: InboundEdgeRemovalViaVertex) => processReturnEdgeRemovalRequest(channelId, channelTime, req) // Excatly the same as above, but for a remote worker

    case TrackedGraphEffect(channelId, channelTime, req: VertexRemoveSyncAck) => processVertexRemoveSyncAck(channelId, channelTime, req)

    case Watermark => processWatermarkRequest(); //println(s"$workerId ${storage.newestTime} ${storage.windowTime} ${storage.newestTime-storage.windowTime}")
    case ProbeWatermark => mediator ! DistributedPubSubMediator.Send("/user/WatermarkManager", WatermarkTime(storage.windowTime), localAffinity = false)
    case req: RouterWorkerTimeSync => processRouterTimeSync(req);
    //case SaveState => serialiseGraphPartition();
    case x => log.warning(s"IngestionWorker [{}] received unknown [{}] message.", workerId, x)
  }


  def processVertexAddRequest(channelId: String, channelTime: Int, update: VertexAdd): Unit = {
    log.debug(s"IngestionWorker [$workerId] received [$update] request.")
    storage.addVertex(update.updateTime, update.srcId, update.properties, update.vType)
    routerUpdates.increment()
    trackVertexAdd(update.updateTime, channelId, channelTime)
  }

  private def trackVertexAdd(msgTime: Long, channelId: String, channelTime: Int): Unit = {
    //Vertex Adds the message time straight into queue as no sync
    storage.timings(msgTime)
    addToWatermarkQueue(channelId, channelTime, msgTime)
  }


  def processEdgeAddRequest(channelId: String, channelTime: Int, update: EdgeAdd): Unit = {
    log.debug(s"IngestionWorker [$workerId] received [$update] request.")
    val maybeEffect = storage.addEdge(update.updateTime, update.srcId, update.dstId, channelId, channelTime, update.properties, update.eType)
    maybeEffect.foreach(sendEffectMessage)
    routerUpdates.increment()
    trackEdgeAdd(update.updateTime, maybeEffect.isEmpty, channelId, channelTime)
  }

  private def trackEdgeAdd(msgTime: Long, local: Boolean, channelId: String, channelTime: Int): Unit = {
    storage.timings(msgTime)
    if (local) { //if the edge is totally handled by this worker then we are safe to add to watermark queue
      addToWatermarkQueue(channelId, channelTime, msgTime)
    }
  }

  def processRemoteEdgeAddRequest(channelId: String, channelTime: Int, req: SyncExistingEdgeAdd): Unit = {
    log.debug(s"IngestionWorker [$workerId] received [$req] request.")
    val effect = storage.syncExistingEdgeAdd(req.msgTime, req.srcId, req.dstId, req.properties, channelId, channelTime)
    sendEffectMessage(effect)
    remoteEdgeAddTrack(req.msgTime)
    interWorkerUpdates.increment()
  }

  def processRemoteEdgeAddNewRequest(channelId: String, channelTime: Int, req: SyncNewEdgeAdd): Unit = {
    log.debug(s"IngestionWorker [$workerId] received [$req] request.")
    val effect = storage.syncNewEdgeAdd(req.msgTime, req.srcId, req.dstId, req.properties, req.kills, req.vType, channelId, channelTime)
    sendEffectMessage(effect)
    remoteEdgeAddTrack(req.msgTime)
    interWorkerUpdates.increment()
  }

  private def remoteEdgeAddTrack(msgTime: Long): Unit = {
    storage.timings(msgTime)
  }
//
def processRemoteReturnDeathsRequest(channelId: String, channelTime: Int, req: SyncExistingRemovals): Unit = { //when the new edge add is responded to we can say it is synced
  log.debug(s"IngestionWorker [$workerId] received [$req] request.")
  storage.syncExistingRemovals(req.msgTime, req.srcId, req.dstId, req.kills)
  addToWatermarkQueue(channelId, channelTime, req.msgTime)
}

  def processEdgeSyncAck(channelId: String, channelTime: Int, req: EdgeSyncAck) = { //when the edge isn't new we will get this response instead
    log.debug(s"IngestionWorker [$workerId] received [$req] request.")
    addToWatermarkQueue(channelId, channelTime, req.msgTime)
  }


  def processEdgeDeleteRequest(channelId: String, channelTime: Int, update: EdgeDelete): Unit = {
    log.debug(s"IngestionWorker [$workerId] received [$update] request.")
    val maybeEffect = storage.removeEdge(update.updateTime, update.srcId, update.dstId, channelId, channelTime)
    maybeEffect.foreach(sendEffectMessage)
    trackEdgeDelete(update.updateTime, maybeEffect.isEmpty, channelId, channelTime)
    routerUpdates.increment()
  }

  private def trackEdgeDelete(msgTime: Long, local: Boolean, channelId: String, channelTime: Int): Unit = {
    storage.timings(msgTime)
    if (local) { //if the edge is totally handled by this worker then we are safe to add to watermark queue
      addToWatermarkQueue(channelId, channelTime, msgTime)
    }
  }

  def processRemoteEdgeRemovalNewRequest(channelId: String, channelTime: Int, req: SyncNewEdgeRemoval): Unit = {
    log.debug(s"IngestionWorker [$workerId] received [$req] request.")
    val effect = storage.syncNewEdgeRemoval(req.msgTime, req.srcId, req.dstId, req.kills, channelId, channelTime)
    sendEffectMessage(effect)
    storage.timings(req.msgTime)
    interWorkerUpdates.increment()
  }

  def processRemoteEdgeRemovalRequest(channelId: String, channelTime: Int, req: SyncExistingEdgeRemoval): Unit = {
    log.debug(s"IngestionWorker [$workerId] received [$req] request.")
    val effect = storage.syncExistingEdgeRemoval(req.msgTime, req.srcId, req.dstId, channelId, channelTime)
    sendEffectMessage(effect)
    storage.timings(req.msgTime)
    interWorkerUpdates.increment()
  }


  def processVertexDeleteRequest(channelId: String, channelTime: Int, update: VertexDelete): Unit = {
    log.debug(s"IngestionWorker [$workerId] received [$update] request.")
    val messages = storage.removeVertex(update.updateTime, update.srcId, channelId, channelTime)
    messages.foreach(x => sendEffectMessage(x))
    trackVertexDelete(update.updateTime, channelId, channelTime, messages.size)
    routerUpdates.increment()
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

  def processRemoteEdgeRemovalRequestFromVertex(channelId: String, channelTime: Int, req: OutboundEdgeRemovalViaVertex): Unit = {
    log.debug(s"IngestionWorker [$workerId] received [$req] request.")
    val effect = storage.outboundEdgeRemovalViaVertex(req.msgTime, req.srcId, req.dstId, channelId, channelTime)
    sendEffectMessage(effect)
    storage.timings(req.msgTime)
    interWorkerUpdates.increment()
  }

  def processReturnEdgeRemovalRequest(channelId: String, channelTime: Int, req: InboundEdgeRemovalViaVertex): Unit = { //remote worker same as above
    log.debug(s"IngestionWorker [$workerId] received [$req] request.")
    val effect = storage.inboundEdgeRemovalViaVertex(req.msgTime, req.srcId, req.dstId, channelId, channelTime)
    sendEffectMessage(effect)
    interWorkerUpdates.increment()
  }

  private def addToWatermarkQueue(channelId:String, channelTime:Int, msgTime:Long) = {
    queuedMessageMap.get(channelId) match {
      case Some(queue) => queue += queueItem(channelTime,msgTime)
      case None =>
        val queue = new mutable.PriorityQueue[queueItem]()(Ordering.by[queueItem, Int](f=>f.routerEpoch).reverse)
        queue += queueItem(channelTime,msgTime)
        queuedMessageMap put(channelId,queue)
    }
    updates+=1
    synchronisedUpdates.increment()
  }



  private def processWatermarkRequest() ={
      if(queuedMessageMap nonEmpty) {
        val queueState = queuedMessageMap.map(queue => {
          setSafePoint(queue._1, queue._2)
        })
        val timestamps = queueState.map(q => q.timestamp)
         // println(s"$increments Writer Worker $partitionID $workerId $timestamps")

//        println(s"Writer Worker $partitionID $workerId ${queueState.mkString("[",",","]")} ${storage.vertices.size}")
        //println(s"$increments Writer Worker $partitionID $workerId ${timestamps.min} ${storage.vertices.size} $updates ${updates-updates2}")
        updates2=updates
        increments+=1

        val min = timestamps.min
        if(storage.windowTime<min)
          storage.windowTime = min
        latestTime.update(storage.newestTime)
        earliestTime.update(storage.oldestTime)
        safeTime.update(storage.windowTime)
      }
  }
  import scala.util.control.Breaks._
  private def setSafePoint(routerName:String,messageQueue:mutable.PriorityQueue[queueItem]) = {

    var currentSafePoint = safeMessageMap.get(routerName) match {
      case Some(value) => value
      case None => queueItem(-1,0)
    }
    breakable {
      while (messageQueue nonEmpty)
        if (messageQueue.head.routerEpoch == currentSafePoint.routerEpoch + 1)
          currentSafePoint = messageQueue.dequeue()
        else if (messageQueue.head.routerEpoch == currentSafePoint.routerEpoch)
          currentSafePoint = messageQueue.dequeue()
        else {
         break
        }
    }
    safeMessageMap put(routerName, currentSafePoint)
    currentSafePoint
  }

  private def processRouterTimeSync(req:RouterWorkerTimeSync) ={
    storage.timings(req.msgTime)
    addToWatermarkQueue(req.routerId,req.routerTime,req.msgTime)
  }

  private def sendEffectMessage[T <: GraphUpdateEffect](msg: TrackedGraphEffect[T]): Unit =
    mediator ! new DistributedPubSubMediator.Send(getManager(msg.effect.updateId, managerCount), msg)


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

}
case class ParquetProperty(key:String,immutable:Boolean,history:List[(Long,String)])
case class ParquetEdge(src:Long, dst:Long, split:Boolean, workerID:Int, history:List[(Long,Boolean)], properties:List[ParquetProperty])
case class ParquetVertex(id:Long, history:List[(Long,Boolean)], properties:List[ParquetProperty], incoming:List[ParquetEdge], outgoing:List[ParquetEdge])
case class StorageState(managerCount:Int,oldestTime: Long,newestTime: Long,windowTime: Long )
object IngestionWorker {
  object Message {
    case object Watermark
  }
}

