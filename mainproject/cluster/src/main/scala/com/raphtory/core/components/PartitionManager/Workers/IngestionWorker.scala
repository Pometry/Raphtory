package com.raphtory.core.components.PartitionManager.Workers

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.model.communication._
import com.raphtory.core.storage.EntityStorage
import com.raphtory.core.utils.SchedulerUtil
import kamon.Kamon

import scala.concurrent.duration._
import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap

case class queueItem(routerEpoch:Int,timestamp:Long)extends Ordered[queueItem] {
  def compare(that: queueItem): Int = that.routerEpoch-this.routerEpoch
}

// TODO Re-add compression (removed during commit Log-Revamp)
class IngestionWorker(workerId: Int,partitionID:Int, storage: EntityStorage) extends Actor with ActorLogging {

  val mediator: ActorRef = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)
  implicit val executionContext = context.system.dispatchers.lookup("worker-dispatcher")

  var increments =0
  var updates = 0
  var updates2 =0

  val synchTime           = Kamon.histogram("Raphtory_Wall_Clock").withTag("actor",s"PartitionWriter_$partitionID").withTag("ID",workerId)
  val routerUpdates       = Kamon.counter("Raphtory_Router_Updates").withTag("actor",s"PartitionWriter_$partitionID").withTag("ID",workerId)
  val interWorkerUpdates  = Kamon.counter("Raphtory_Inter_Worker_Updates").withTag("actor",s"PartitionWriter_$partitionID").withTag("ID",workerId)
  val intraWorkerUpdates  = Kamon.counter("Raphtory_Intra_Worker_Updates").withTag("actor",s"PartitionWriter_$partitionID").withTag("ID",workerId)
  val synchronisedUpdates = Kamon.counter("Raphtory_Synchronised_Updates").withTag("actor",s"PartitionWriter_$partitionID").withTag("ID",workerId)

  val safeTime            = Kamon.gauge("Raphtory_Safe_Time").withTag("actor",s"PartitionWriter_$partitionID").withTag("ID",workerId)
  val latestTime          = Kamon.gauge("Raphtory_Latest_Time").withTag("actor",s"PartitionWriter_$partitionID").withTag("ID",workerId)
  val earliestTime        = Kamon.gauge("Raphtory_Earliest_Time").withTag("actor",s"PartitionWriter_$partitionID").withTag("ID",workerId)


  private val queuedMessageMap = ParTrieMap[String, mutable.PriorityQueue[queueItem]]()
  private val safeMessageMap = ParTrieMap[String, queueItem]()
  private val vDeleteCountdownMap = ParTrieMap[(String,Int), AtomicInteger]()

  override def receive: Receive = {
    case req: TrackedVertexAdd                  => processVertexAddRequest(req);//Add a new vertex
    case req: TrackedVertexAddWithProperties    => processVertexAddWithPropertiesRequest(req)//Add a new vertex with properties
    case req: TrackedEdgeAdd                    => processEdgeAddRequest(req)//Add an edge
    case req: TrackedEdgeAddWithProperties      => processEdgeAddWithPropertiesRequest(req)// Add an edge with properties

    case req: RemoteEdgeAddNew           => processRemoteEdgeAddNewRequest(req)//A writer has requested a new edge sync for a destination node in this worker
    case req: RemoteEdgeAdd              => processRemoteEdgeAddRequest(req)// A writer has requested an existing edge sync for a destination node on in this worker
    case req: RemoteReturnDeaths         => processRemoteReturnDeathsRequest(req)//The remote worker has returned all removals in the destination node -- for new edges
    case req: EdgeSyncAck                => processEdgeSyncAck(req) //The remote worker acknowledges the completion of an edge sync

    case req: DstAddForOtherWorker       => processDstAddForOtherWorkerRequest(req) //A local writer has requested a new edge sync for a destination node in this worker
    case req: DstResponseFromOtherWorker => processDstResponseFromOtherWorkerRequest(req)//The local writer has responded with the deletions for local split edge to allow the main writer to insert them

    case req: TrackedEdgeDelete          => processEdgeDeleteRequest(req) //Delete an Edge
    case req: RemoteEdgeRemovalNew       => processRemoteEdgeRemovalNewRequest(req) //A remote worker is asking for a new edge to be removed for a destination node in this worker
    case req: RemoteEdgeRemoval          => processRemoteEdgeRemovalRequest(req) //A remote worker is asking for the deletion of an existing edge
    case req: DstWipeForOtherWorker      => processDstWipeForOtherWorkerRequest(req)//A local worker is asking for a new edge sync for a destination node for this worker

    case req: TrackedVertexDelete        => processVertexDeleteRequest(req) //Delete a vertex and all associated edges
    case req: RemoteEdgeRemovalFromVertex=> processRemoteEdgeRemovalRequestFromVertex(req) //Does exactly the same as above, but for when the removal comes form a vertex
    case req: EdgeRemoveForOtherWorker   => processEdgeRemoveForOtherWorkerRequest(req) //Handle the deletion of an outgoing edge from a vertex deletion on a local worker
    case req: ReturnEdgeRemoval          => processReturnEdgeRemovalRequest(req) // Excatly the same as above, but for a remote worker

    case req:VertexRemoveSyncAck         => processVertexRemoveSyncAck(req)

    case "watermark"                     => processWatermarkRequest(); //println(s"$workerId ${storage.newestTime} ${storage.windowTime} ${storage.newestTime-storage.windowTime}")
    case req:RouterWorkerTimeSync        => processRouterTimeSync(req);
    case x =>
      log.warning(s"IngestionWorker [{}] received unknown [{}] message.", workerId, x)
  }


  def processVertexAddRequest(req: TrackedVertexAdd): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    val update = req.update
    storage.vertexAdd(update.msgTime, update.srcID, vertexType = update.vType)
    routerUpdates.increment()
    vertexAddTrack(update.srcID, update.msgTime,req.routerID,req.messageID,spoutTime = req.spoutTime)
  }

  def processVertexAddWithPropertiesRequest(req: TrackedVertexAddWithProperties): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    val update = req.update
    storage.vertexAdd(update.msgTime, update.srcID, update.properties, update.vType)
    routerUpdates.increment()
    vertexAddTrack(update.srcID, update.msgTime,req.routerID,req.messageID,req.spoutTime)
  }

  private def vertexAddTrack(srcID: Long, msgTime: Long,routerID:String,routerTime:Int,spoutTime:Long): Unit = {
      //Vertex Adds the message time straight into queue as no sync
    storage.timings(msgTime)
    addToWatermarkQueue(routerID,routerTime,msgTime,spoutTime = spoutTime)
  }


  def processEdgeAddRequest(req: TrackedEdgeAdd): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    val update = req.update
    val local = storage.edgeAdd(update.msgTime, update.srcID, update.dstID,req.routerID,req.messageID, edgeType = update.eType,spoutTime=req.spoutTime)
    routerUpdates.increment()
    edgeAddTrack(update.srcID, update.dstID, update.msgTime,local,req.routerID,req.messageID,spoutTime = req.spoutTime)
  }

  def processEdgeAddWithPropertiesRequest(req: TrackedEdgeAddWithProperties): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    val update = req.update
    val local = storage.edgeAdd(update.msgTime, update.srcID, update.dstID,req.routerID,req.messageID, update.properties, update.eType,spoutTime=req.spoutTime)
    routerUpdates.increment()
    edgeAddTrack(update.srcID, update.dstID, update.msgTime,local,req.routerID,req.messageID,req.spoutTime)
  }

  private def edgeAddTrack(srcID: Long, dstID: Long, msgTime: Long,local:Boolean,routerID:String,routerTime:Int,spoutTime:Long): Unit = {
    storage.timings(msgTime)
    if(local) {//if the edge is totally handled by this worker then we are safe to add to watermark queue
      addToWatermarkQueue(routerID,routerTime,msgTime,spoutTime = spoutTime)
    }
  }

  def processRemoteEdgeAddRequest(req: RemoteEdgeAdd): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    storage.remoteEdgeAdd(req.msgTime, req.srcID, req.dstID, req.properties, req.eType,req.routerID,req.routerTime,spoutTime=req.spoutTime)
    remoteEdgeAddTrack(req.msgTime)
    interWorkerUpdates.increment()
  }

  def processRemoteEdgeAddNewRequest(req: RemoteEdgeAddNew): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    storage.remoteEdgeAddNew(req.msgTime, req.srcID, req.dstID, req.properties, req.kills, req.vType,req.routerID,req.routerTime,spoutTime=req.spoutTime)
    remoteEdgeAddTrack(req.msgTime)
    interWorkerUpdates.increment()
  }

  private def remoteEdgeAddTrack(msgTime: Long): Unit = {
    storage.timings(msgTime)
  }
//
  def processRemoteReturnDeathsRequest(req: RemoteReturnDeaths): Unit = { //when the new edge add is responded to we can say it is synced
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    storage.remoteReturnDeaths(req.msgTime, req.srcID, req.dstID, req.kills)
    addToWatermarkQueue(req.routerID,req.routerTime,req.msgTime,spoutTime = req.spoutTime)
  }

  def processEdgeSyncAck(req: EdgeSyncAck) = { //when the edge isn't new we will get this response instead
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    addToWatermarkQueue(req.routerID,req.routerTime,req.msgTime,spoutTime = req.spoutTime)
  }

  def processDstAddForOtherWorkerRequest(req: DstAddForOtherWorker): Unit = { //local worker asking this one to deal with an incoming edge
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    storage.vertexWorkerRequest(req.msgTime, req.dstID, req.srcForEdge, req.edge, req.present,req.routerID,req.routerTime,spoutTime=req.spoutTime)
    storage.timings(req.msgTime)
    intraWorkerUpdates.increment()
  }

  def processDstResponseFromOtherWorkerRequest(req: DstResponseFromOtherWorker): Unit = { //local worker responded for a new edge so can watermark, if existing edge will just be an ack
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    storage.vertexWorkerRequestEdgeHandler(req.msgTime, req.srcForEdge, req.dstID, req.removeList)
    addToWatermarkQueue(req.routerID,req.routerTime,req.msgTime,spoutTime = req.spoutTime)
  }


  def processEdgeDeleteRequest(req: TrackedEdgeDelete): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    val update = req.update
    val local = storage.edgeRemoval(update.msgTime, update.srcID, update.dstID,req.routerID,req.messageID,spoutTime=req.spoutTime)
    edgeDeleteTrack(update.msgTime,local,req.routerID,req.messageID,req.spoutTime)
    routerUpdates.increment()
  }

  private def edgeDeleteTrack(msgTime: Long,local:Boolean,routerID:String,routerTime:Int,spoutTime:Long): Unit = {
    storage.timings(msgTime)
    if(local) { //if the edge is totally handled by this worker then we are safe to add to watermark queue
      addToWatermarkQueue(routerID,routerTime,msgTime,spoutTime = spoutTime)
    }
  }

  def processRemoteEdgeRemovalNewRequest(req: RemoteEdgeRemovalNew): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    storage.remoteEdgeRemovalNew(req.msgTime, req.srcID, req.dstID, req.kills,req.routerID,req.routerTime,spoutTime=req.spoutTime)
    storage.timings(req.msgTime)
    interWorkerUpdates.increment()
  }

  def processRemoteEdgeRemovalRequest(req: RemoteEdgeRemoval): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    storage.remoteEdgeRemoval(req.msgTime, req.srcID, req.dstID,req.routerID,req.routerTime,spoutTime=req.spoutTime)
    storage.timings(req.msgTime)
    interWorkerUpdates.increment()
  }

  def processDstWipeForOtherWorkerRequest(req: DstWipeForOtherWorker): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    storage.vertexWipeWorkerRequest(req.msgTime, req.dstID, req.srcForEdge, req.edge, req.present,req.routerID,req.routerTime,spoutTime=req.spoutTime)
    storage.timings(req.msgTime)
    intraWorkerUpdates.increment()
  }


  def processVertexDeleteRequest(req: TrackedVertexDelete): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    val update = req.update
    val totalCount = storage.vertexRemoval(update.msgTime, update.srcID,req.routerID,req.messageID,spoutTime=req.spoutTime)
    vertexDeleteTrack(update.srcID, update.msgTime,req.routerID,req.messageID,totalCount,req.spoutTime)
    routerUpdates.increment()
  }
  private def vertexDeleteTrack(srcID: Long, msgTime: Long,routerID:String,routerTime:Int,totalCount:Int,spoutTime:Long): Unit = {
    if(totalCount==0) //if there are no outgoing edges it is safe to watermark
      addToWatermarkQueue(routerID,routerTime,msgTime,spoutTime = spoutTime)
    else{
      vDeleteCountdownMap put ((routerID,routerTime),new AtomicInteger(totalCount))
    }
    storage.timings(msgTime)
  }

  def processVertexRemoveSyncAck(req:VertexRemoveSyncAck)= {
    vDeleteCountdownMap.get((req.routerID,req.routerTime)) match {
      case Some(integer) => if(integer.decrementAndGet()==0){
        addToWatermarkQueue(req.routerID,req.routerTime,req.msgTime,spoutTime = req.spoutTime)
        vDeleteCountdownMap.remove((req.routerID,req.routerTime)) //todo improve this datastructure
      }
      case None          => println("Should never Happen")
    }
  }


  def processRemoteEdgeRemovalRequestFromVertex(req: RemoteEdgeRemovalFromVertex): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    storage.remoteEdgeRemovalFromVertex(req.msgTime, req.srcID, req.dstID,req.routerID,req.routerTime,spoutTime=req.spoutTime)
    storage.timings(req.msgTime)
    interWorkerUpdates.increment()
  }

  def processEdgeRemoveForOtherWorkerRequest(req: EdgeRemoveForOtherWorker): Unit = { //local worker has destination and needs this worker to sort the edge removal
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    storage.edgeRemovalFromOtherWorker(req.msgTime, req.srcID, req.dstID,req.routerID,req.routerTime,spoutTime=req.spoutTime)
    intraWorkerUpdates.increment()
  }

  def processReturnEdgeRemovalRequest(req: ReturnEdgeRemoval): Unit = { //remote worker same as above
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    storage.returnEdgeRemoval(req.msgTime, req.srcID, req.dstID,req.routerID,req.routerTime,spoutTime=req.spoutTime)
    interWorkerUpdates.increment()
  }

  private def addToWatermarkQueue(routerID:String,routerTime:Int,msgTime:Long,spoutTime:Long) = {
    queuedMessageMap.get(routerID) match {
      case Some(queue) => queue += queueItem(routerTime,msgTime)
      case None =>
        val queue = new mutable.PriorityQueue[queueItem]()(Ordering.by[queueItem, Int](f=>f.routerEpoch).reverse)
        queue += queueItem(routerTime,msgTime)
        queuedMessageMap put(routerID,queue)
    }
    updates+=1
    synchronisedUpdates.increment()
    processSynchTime(spoutTime)
  }

  def processSynchTime(spoutTime:Long):Unit = if(spoutTime>0) {
    synchTime.record(System.currentTimeMillis()-spoutTime)
  }

  private def processWatermarkRequest() ={
      if(queuedMessageMap nonEmpty) {
        val queueState = queuedMessageMap.map(queue => {
          setSafePoint(queue._1, queue._2)
        })
        val timestamps = queueState.map(q => q.timestamp)
        if(workerId==1)
          println(s"$increments Writer Worker $partitionID $workerId $timestamps")

        //println(s"Writer Worker $partitionID $workerId ${queueState.mkString("[",",","]")} ${storage.vertices.size}")
        //println(s"$increments Writer Worker $partitionID $workerId ${timestamps.min} ${storage.vertices.size} $updates ${updates-updates2}")
        updates2=updates
        increments+=1

        val min = timestamps.min
        if(storage.windowTime<min)
          storage.windowTime = min
        mediator ! DistributedPubSubMediator.Send("/user/WatermarkManager",WatermarkTime(storage.windowTime), localAffinity = false)
        latestTime.update(storage.newestTime)
        earliestTime.update(storage.oldestTime)
        safeTime.update(storage.windowTime)
      }
  }

  private def setSafePoint(routerName:String,messageQueue:mutable.PriorityQueue[queueItem]) = {
    val default = queueItem(-1,0)
    var currentSafePoint = safeMessageMap.get(routerName) match {
      case Some(value) => value
      case None => default
    }
    if(messageQueue nonEmpty)
      while(messageQueue.headOption.getOrElse(default).routerEpoch==currentSafePoint.routerEpoch+1) {
        currentSafePoint = messageQueue.dequeue()
        if(workerId==1 && (routerName equals "0_3"))
          println(currentSafePoint)
      }
    safeMessageMap put (routerName,currentSafePoint)

    currentSafePoint
  }

  def recursiveDequeue(q:mutable.PriorityQueue[queueItem])

  private def processRouterTimeSync(req:RouterWorkerTimeSync) ={
    storage.timings(req.msgTime)
    addToWatermarkQueue(req.routerID,req.routerTime,req.msgTime,0)
  }


  private val scheduledTaskMap: mutable.HashMap[String, Cancellable] = mutable.HashMap[String, Cancellable]()
  override def preStart() {
    log.debug("IngestionWorker is being started.")
    scheduleTasks()
  }
  override def postStop() {
    val allTasksCancelled = scheduledTaskMap.forall {
      case (key, task) =>
        SchedulerUtil.cancelTask(key, task)
    }
    if (!allTasksCancelled) log.warning("Failed to cancel all scheduled tasks post stop.")
  }
  private def scheduleTasks(): Unit = {
    log.debug("Preparing to schedule tasks in Spout.")
    val watermarkCancellable =
      SchedulerUtil.scheduleTask(initialDelay = 10 seconds, interval = 5 second, receiver = self, message = "watermark")
    scheduledTaskMap.put("watermark", watermarkCancellable)

  }

  }

