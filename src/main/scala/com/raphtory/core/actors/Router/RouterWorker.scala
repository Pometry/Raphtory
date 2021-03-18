package com.raphtory.core.actors.Router

import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import akka.util.Timeout
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.actors.Router.RouterWorker.CommonMessage.{DataFinishedSync, RouterWorkerTimeSync, StartUp, TimeBroadcast}
import com.raphtory.core.actors.Router.RouterWorker.State
import com.raphtory.core.actors.Spout.SpoutAgent.CommonMessage.{AllocateTuple, DataFinished, NoWork, SpoutOnline, WorkPlease}
import com.raphtory.core.model.communication._
import kamon.Kamon
import akka.pattern.ask
import com.raphtory.core.actors.ClusterManagement.RaphtoryReplicator.Message.UpdatedCounter
import com.raphtory.core.actors.ClusterManagement.WatchDog.Message.{ClusterStatusRequest, ClusterStatusResponse}

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap
import scala.concurrent.{Await, ExecutionContext}
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
  //println(s"Router $routerId $workerID with $initialManagerCount $initialRouterCount")
  private val messageIDs = ParTrieMap[String, Int]()
  private var safe = false

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
    context.system.scheduler.scheduleOnce(Duration(10, SECONDS), self, StartUp) // wait 10 seconds to start

  }

  override def receive: Receive = work(State(initialManagerCount, 0L, false,0L))

  private def work(state: State): Receive = {
    case StartUp     =>
      if (!safe) {
        mediator ! new DistributedPubSubMediator.Send("/user/WatchDog", ClusterStatusRequest) //ask if the cluster is safe to use
        context.system.scheduler.scheduleOnce(1.second, self, StartUp)  // repeat every 1 second until safe
      }
    case ClusterStatusResponse(clusterUp, _, _) =>
      if (clusterUp) safe = true

    case SpoutOnline => context.sender() ! WorkPlease
    case NoWork =>
      context.system.scheduler.scheduleOnce(delay = 1.second, receiver = context.sender(), message = WorkPlease)

    case msg: UpdatedCounter =>
      log.debug(s"RouterWorker [$routerId] received [$msg] request.")
      if (state.managerCount < msg.newValue) context.become(work(state.copy(managerCount = msg.newValue)))

    case AllocateTuple(record: T) => //todo: wvv AllocateTuple should hold type of record instead of using Any
      log.debug(s"RouterWorker [$routerId] received AllocateTuple[$record] request.")
      val newNewestTimes = parseTupleAndSendGraph(record, state.managerCount)
      val newNewestTime  = (state.newestTime :: newNewestTimes).max
      if (newNewestTime > state.newestTime)
        context.become(work(state.copy(newestTime = newNewestTime)))
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
        if (state.restRouterNewestFinishedTime > state.newestTime) {
          broadcastRouterWorkerTimeSync(state.managerCount, state.restRouterNewestFinishedTime)
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
      managerCount: Int
  ): List[Long] =
    graphBuilder.getUpdates(record).map(update => sendGraphUpdate(update, managerCount))

  private def sendGraphUpdate(
      message: GraphUpdate,
      managerCount: Int,
  ): Long = {
    update += 1
    routerWorkerUpdates.increment()
    val path             = getManager(message.srcId, managerCount)
    val id               = getMessageIDForWriter(path)

    val sentMessage = TrackedGraphUpdate(s"${routerId}_$workerID", id, message)
    log.debug(s"RouterWorker sending message [$sentMessage] to PubSub")

    mediator ! DistributedPubSubMediator.Send(path, sentMessage, localAffinity = false)
    message.updateTime
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
    if(safe) {
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

}



object RouterWorker {
  object CommonMessage {
    case object StartUp
    case object TimeBroadcast
    case class DataFinishedSync(time:Long)
    case class RouterWorkerTimeSync(msgTime:Long, routerId:String, routerTime:Int)
  }

  private case class State(
                            managerCount: Int,
                            newestTime: Long,
                            dataFinished: Boolean,
                            restRouterNewestFinishedTime: Long
  )
}
