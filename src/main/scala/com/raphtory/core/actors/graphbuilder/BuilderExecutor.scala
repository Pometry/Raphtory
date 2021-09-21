package com.raphtory.core.actors.graphbuilder

import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import akka.util.Timeout
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.actors.graphbuilder.BuilderExecutor.CommonMessage.{DataFinishedSync, KeepAlive, RouterWorkerTimeSync, StartUp, TimeBroadcast}
import com.raphtory.core.actors.graphbuilder.BuilderExecutor.State
import com.raphtory.core.actors.spout.SpoutAgent.CommonMessage.{AllocateTuple, DataFinished, NoWork, SpoutOnline, WorkPlease}
import com.raphtory.core.model.communication._
import akka.pattern.ask
import com.raphtory.core.actors.orchestration.clustermanager.WatchDog.Message.{ClusterStatusRequest, ClusterStatusResponse, RouterUp}

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

// TODO Add val name which sub classes that extend this trait must overwrite
//  e.g. BlockChainRouter val name = "Blockchain Router"
//  Log.debug that read 'Router' should then read 'Blockchain Router'
class BuilderExecutor[T](
    val graphBuilder: GraphBuilder[T],
    val routerId: Int,
) extends RaphtoryActor {
  implicit val executionContext: ExecutionContext = context.system.dispatcher
  //println(s"Router $routerId $workerID with $initialManagerCount $initialRouterCount")
  private val messageIDs = ParTrieMap[String, Int]()
  private var safe = false

  var update = 0
  // todo: wvv let people know parseTuple will create a list of update message
  //  and this trait will handle logic to send to graph

  final protected val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)

  override def preStart(): Unit = {
    log.debug(s"RouterWorker [$routerId] is being started.")
    context.system.scheduler.scheduleOnce(delay = 5.seconds, receiver = self, message = TimeBroadcast)
    context.system.scheduler.scheduleOnce(Duration(10, SECONDS), self, StartUp) // wait 10 seconds to start
    context.system.scheduler.schedule(0 seconds, 10 seconds, self, KeepAlive)


  }

  override def receive: Receive = work(State(0L, false,0L))

  private def work(state: State): Receive = {
    case KeepAlive =>
      mediator ! DistributedPubSubMediator.Send("/user/WatchDog", RouterUp(routerId), localAffinity = false)

    case StartUp     =>
      if (!safe) {
        mediator ! new DistributedPubSubMediator.Send("/user/WatchDog", ClusterStatusRequest) //ask if the cluster is safe to use
        context.system.scheduler.scheduleOnce(1.second, self, StartUp)  // repeat every 1 second until safe
      }
    case ClusterStatusResponse(clusterUp) =>
      if (clusterUp) safe = true

    case SpoutOnline => context.sender() ! WorkPlease
    case NoWork =>
      context.system.scheduler.scheduleOnce(delay = 1.second, receiver = context.sender(), message = WorkPlease)

    case AllocateTuple(record: T) => //todo: wvv AllocateTuple should hold type of record instead of using Any
      log.debug(s"RouterWorker [$routerId] received AllocateTuple[$record] request.")
      val newNewestTimes = parseTupleAndSendGraph(record)
      val newNewestTime  = (state.newestTime :: newNewestTimes).max
      if (newNewestTime > state.newestTime)
        context.become(work(state.copy(newestTime = newNewestTime)))
      context.sender() ! WorkPlease

    case TimeBroadcast =>
      broadcastRouterWorkerTimeSync(state.newestTime)
      context.system.scheduler.scheduleOnce(delay = 5.seconds, receiver = self, message = TimeBroadcast)

    case DataFinished =>
      getAllRouterWorkers().foreach { workerPath =>
        mediator ! new DistributedPubSubMediator.Send(
                workerPath,
                DataFinishedSync(state.newestTime)
        )
        if (state.restRouterNewestFinishedTime > state.newestTime) {
          broadcastRouterWorkerTimeSync(state.restRouterNewestFinishedTime)
        }
        val newNewestTime = state.newestTime max state.restRouterNewestFinishedTime
        context.become(work(state.copy(newestTime = newNewestTime, dataFinished = true)))
      }

    case DataFinishedSync(time) =>
      if (state.dataFinished) {
        if (time > state.newestTime) {
          broadcastRouterWorkerTimeSync(time)
          context.become(work(state.copy(newestTime = time)))
        }
      } else {
        context.become(work(state.copy(restRouterNewestFinishedTime = time max state.restRouterNewestFinishedTime)))
      }
    case unhandled => log.warning(s"RouterWorker received unknown [$unhandled] message.")
  }

  private def parseTupleAndSendGraph(record: T): List[Long] =
    graphBuilder.getUpdates(record).map(update => sendGraphUpdate(update))

  private def sendGraphUpdate(message: GraphUpdate): Long = {
    update += 1
    val path             = getWriter(message.srcId)
    val id               = getMessageIDForWriter(path)

    val sentMessage = TrackedGraphUpdate(s"$routerId", id, message)
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

  
  def broadcastRouterWorkerTimeSync(time: Long) = {
    if(safe) {
        getAllWriters().foreach { workerPath =>
        mediator ! new DistributedPubSubMediator.Send(
          workerPath,
          RouterWorkerTimeSync(time, s"$routerId", getMessageIDForWriter(workerPath))
        )
      }
    }
  }

}



object BuilderExecutor {
  object CommonMessage {
    case object StartUp
    case object TimeBroadcast
    case object KeepAlive
    case class DataFinishedSync(time:Long)
    case class RouterWorkerTimeSync(msgTime:Long, routerId:String, routerTime:Int)
  }

  private case class State(
                            newestTime: Long,
                            dataFinished: Boolean,
                            restRouterNewestFinishedTime: Long
  )
}
