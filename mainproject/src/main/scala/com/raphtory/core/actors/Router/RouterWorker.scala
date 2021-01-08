package com.raphtory.core.actors.Router

import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.actors.Router.RouterWorker.CommonMessage.TimeBroadcast
import com.raphtory.core.actors.Router.RouterWorker.State
import com.raphtory.core.actors.Spout.SpoutAgent.CommonMessage.NoWork
import com.raphtory.core.actors.Spout.SpoutAgent.CommonMessage.SpoutOnline
import com.raphtory.core.actors.Spout.SpoutAgent.CommonMessage.WorkPlease
import com.raphtory.core.model.communication._
import kamon.Kamon

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

// TODO Add val name which sub classes that extend this trait must overwrite
//  e.g. BlockChainRouter val name = "Blockchain Router"
//  Log.debug that read 'Router' should then read 'Blockchain Router'
class RouterWorker[T](
    val graphBuilder: GraphBuilder[T],
    val routerId: Int,
    val workerID: Int,
    val initialManagerCount: Int,
    val initialRouterCount: Int
) extends RaphtoryActor {
  implicit val executionContext: ExecutionContext = context.system.dispatcher
//  println(s"Router $routerId $workerID with $initialManagerCount $initialRouterCount")
  private val messageIDs = ParTrieMap[String, Int]()

  private val routerWorkerUpdates =
    Kamon.counter("Raphtory_Router_Output").withTag("Router", routerId).withTag("Worker", workerID)
  var update = 0
  // todo: wvv let people know parseTuple will create a list of update message
  //  and this trait will handle logic to send to graph

  final protected val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)

  override def preStart(): Unit = {
    log.debug(s"RouterWorker [$routerId] is being started.")
    context.system.scheduler.scheduleOnce(delay = 5.seconds, receiver = self, message = TimeBroadcast)
  }

  override def receive: Receive = work(State(initialManagerCount, 0L, 0L, false,0L))

  private def work(state: State): Receive = {
    case SpoutOnline => context.sender() ! WorkPlease
    case NoWork =>
      context.system.scheduler.scheduleOnce(delay = 1.second, receiver = context.sender(), message = WorkPlease)

    case msg: UpdatedCounter =>
      log.debug(s"RouterWorker [$routerId] received [$msg] request.")
      if (state.managerCount < msg.newValue) context.become(work(state.copy(managerCount = msg.newValue)))

    case AllocateTuple(record: T) => //todo: wvv AllocateTuple should hold type of record instead of using Any
      log.debug(s"RouterWorker [$routerId] received AllocateTuple[$record] request.")
      val newNewestTimes = parseTupleAndSendGraph(record, state.managerCount, false, state.trackedTime)
      val newNewestTime  = (state.newestTime :: newNewestTimes).max
      if (newNewestTime > state.newestTime)
        context.become(work(state.copy(newestTime = newNewestTime)))
      context.sender() ! WorkPlease

    case msg @ AllocateTrackedTuple(
                wallClock,
                record: T
        ) => //todo: wvv AllocateTrackedTuple should hold type of record instead of using Any
      log.debug(s"RouterWorker [$routerId] received [$msg] request.")
      val newNewestTimes = parseTupleAndSendGraph(record, state.managerCount, true, wallClock)
      val newNewestTime  = (state.newestTime :: newNewestTimes).max
      if (newNewestTime > state.newestTime)
        context.become(work(state.copy(trackedTime = wallClock, newestTime = newNewestTime)))
      context.sender() ! WorkPlease

    case TimeBroadcast =>
      broadcastRouterWorkerTimeSync(state.managerCount, state.newestTime)
      context.system.scheduler.scheduleOnce(delay = 5.seconds, receiver = self, message = TimeBroadcast)

    case DataFinished =>
      getAllRouterWorkers(initialRouterCount).foreach { workerPath =>
        mediator ! new DistributedPubSubMediator.Send(
                workerPath,
                DataFinishedSync(state.newestTime)
        )
      }
    }

    case DataFinishedSync(time) => {
      if (time >= newestTime) {
//        println(s"Router $routerId $workerID ${time}")
        getAllWriterWorkers(managerCount).foreach { workerPath =>
          mediator ! DistributedPubSubMediator.Send(
            workerPath,
            RouterWorkerTimeSync(time, s"${routerId}_$workerID", getMessageIDForWriter(workerPath)),
            false
          )
        }
        val newNewestTime = state.newestTime max state.restRouterNewestFinishedTime
        context.become(work(state.copy(newestTime = newNewestTime, dataFinished = true)))
      }

    case DataFinishedSync(time) =>
      if (state.dataFinished) {
        if (time > state.newestTime) {
          broadcastRouterWorkerTimeSync(state.managerCount, time)
          context.become(work(state.copy(newestTime = time)))
        }
      } else {
        context.become(work(state.copy(restRouterNewestFinishedTime = time max state.restRouterNewestFinishedTime)))
      }
    case unhandled => log.warning(s"RouterWorker received unknown [$unhandled] message.")
  }

  private def parseTupleAndSendGraph(
      record: T,
      managerCount: Int,
      trackedMessage: Boolean,
      trackedTime: Long
  ): List[Long] =
    graphBuilder.getUpdates(record).map(update => sendGraphUpdate(update, managerCount, trackedMessage, trackedTime))

  private def sendGraphUpdate(
      message: GraphUpdate,
      managerCount: Int,
      trackedMessage: Boolean,
      trackedTime: Long
  ): Long = {
    update += 1
    routerWorkerUpdates.increment()
    val path             = getManager(message.srcID, managerCount)
    val id               = getMessageIDForWriter(path)
    val trackedTimeToUse = if (trackedMessage) trackedTime else -1L

    val sentMessage = message match {
      case m: VertexAdd =>
        TrackedVertexAdd(s"${routerId}_$workerID", id, trackedTimeToUse, m)
      case m: VertexAddWithProperties =>
        TrackedVertexAddWithProperties(s"${routerId}_$workerID", id, trackedTimeToUse, m)
      case m: EdgeAdd =>
        TrackedEdgeAdd(s"${routerId}_$workerID", id, trackedTimeToUse, m)
      case m: EdgeAddWithProperties =>
        TrackedEdgeAddWithProperties(s"${routerId}_$workerID", id, trackedTimeToUse, m)
      case m: VertexDelete =>
        TrackedVertexDelete(s"${routerId}_$workerID", id, trackedTimeToUse, m)
      case m: EdgeDelete =>
        TrackedEdgeDelete(s"${routerId}_$workerID", id, trackedTimeToUse, m)
    }
    log.debug(s"RouterWorker sending message [$sentMessage] to PubSub")
    if (trackedMessage)
      mediator ! DistributedPubSubMediator
        .Send("/user/WatermarkManager", UpdateArrivalTime(trackedTime, message.msgTime), localAffinity = false)

    mediator ! DistributedPubSubMediator.Send(path, sentMessage, localAffinity = false)
    message.msgTime
  }

  private def getMessageIDForWriter(path: String) =
    messageIDs.get(path) match {
      case Some(messageId) =>
        messageIDs put (path, messageId + 1)
        messageId
      case None =>
        messageIDs put (path, 1)
        0
    }

  def getAllWriterWorkers(managerCount: Int): Array[String] = {
    val workers = mutable.ArrayBuffer[String]()
    for (i <- 0 until managerCount)
      for (j <- 0 until totalWorkers)
        workers += s"/user/Manager_${i}_child_$j"
    workers.toArray
  }

  def broadcastRouterWorkerTimeSync(managerCount: Int, time: Long) = {
    val workerPaths = for {
      i <- 0 until managerCount
      j <- 0 until totalWorkers
    } yield s"/user/Manager_${i}_child_$j"

    workerPaths.foreach { workerPath =>
      mediator ! new DistributedPubSubMediator.Send(
        workerPath,
        RouterWorkerTimeSync(time, s"${routerId}_$workerID", getMessageIDForWriter(workerPath))
      )
    }
  }
}

object RouterWorker {
  object CommonMessage {
    case object TimeBroadcast
  }

  private case class State(
                            managerCount: Int,
                            trackedTime: Long,
                            newestTime: Long,
                            dataFinished: Boolean,
                            restRouterNewestFinishedTime: Long
  )
}
