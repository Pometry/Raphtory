package com.raphtory.core.components.graphbuilder

import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import akka.util.Timeout
import com.raphtory.core.components.graphbuilder.BuilderExecutor.Message.{BuilderTimeSync, DataFinishedSync, KeepAlive, StartUp, TimeBroadcast}
import com.raphtory.core.components.graphbuilder.BuilderExecutor.State
import com.raphtory.core.components.spout.SpoutAgent.Message.{AllocateTuples, DataFinished, NoWork, SpoutOnline, WorkPlease}
import com.raphtory.core.implementations.generic.messaging._
import akka.pattern.ask
import com.raphtory.core.components.akkamanagement.RaphtoryActor
import com.raphtory.core.components.leader.WatchDog.Message.{BuilderUp, ClusterStatusRequest, ClusterStatusResponse}
import com.raphtory.core.model.graph.{GraphUpdate, TrackedGraphUpdate}

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

class BuilderExecutor[T](val graphBuilder: GraphBuilder[T], val builderID: Int) extends RaphtoryActor {
  private val messageIDs = mutable.Map[String, Int]()
  private var safe = false

  var update = 0

  override def preStart(): Unit = {
    log.debug(s"Builder Executor [$builderID] is being started.")
    context.system.scheduler.scheduleOnce(delay = 5.seconds, receiver = self, message = TimeBroadcast)
    context.system.scheduler.scheduleOnce(Duration(10, SECONDS), self, StartUp) // wait 10 seconds to start
    context.system.scheduler.schedule(0 seconds, 10 seconds, self, KeepAlive)
  }

  override def receive: Receive = work(State(0L, false,0L))

  private def work(state: State): Receive = {
    case KeepAlive =>
      mediator ! DistributedPubSubMediator.Send("/user/WatchDog", BuilderUp(builderID), localAffinity = false)

    case StartUp   =>
      if (!safe) {
        mediator ! new DistributedPubSubMediator.Send("/user/WatchDog", ClusterStatusRequest) //ask if the cluster is safe to use
        context.system.scheduler.scheduleOnce(1.second, self, StartUp)  // repeat every 1 second until safe
      }
    case ClusterStatusResponse(clusterUp) =>
      if (clusterUp) safe = true

    case SpoutOnline => context.sender() ! WorkPlease
    case NoWork =>
      context.system.scheduler.scheduleOnce(delay = 1.second, receiver = context.sender(), message = WorkPlease)

    case e:AllocateTuples[T] => //todo: wvv AllocateTuple should hold type of record instead of using Any
      val newNewestTimes = (
        for(record <- e.record)
          yield parseTupleAndSendGraph(record)
        ).flatten.toList

      try{
        val newNewestTime  = (state.newestTime :: newNewestTimes).max
        if (newNewestTime > state.newestTime)
          context.become(work(state.copy(newestTime = newNewestTime)))
      }catch {
        case e:Exception => println("error")
      }

      context.sender() ! WorkPlease

    case TimeBroadcast =>
      broadcastBuilderTimeSync(state.newestTime)
      context.system.scheduler.scheduleOnce(delay = 5.seconds, receiver = self, message = TimeBroadcast)

    case DataFinished =>
      getAllGraphBuilders().foreach { workerPath =>
        mediator ! new DistributedPubSubMediator.Send(
                workerPath,
                DataFinishedSync(state.newestTime)
        )
        if (state.restBuilderNewestFinishedTime > state.newestTime) {
          broadcastBuilderTimeSync(state.restBuilderNewestFinishedTime)
        }
        val newNewestTime = state.newestTime max state.restBuilderNewestFinishedTime
        context.become(work(state.copy(newestTime = newNewestTime, dataFinished = true)))
      }

    case DataFinishedSync(time) =>
      if (state.dataFinished) {
        if (time > state.newestTime) {
          broadcastBuilderTimeSync(time)
          context.become(work(state.copy(newestTime = time)))
        }
      } else {
        context.become(work(state.copy(restBuilderNewestFinishedTime = time max state.restBuilderNewestFinishedTime)))
      }
    case unhandled => log.warning(s"Builder Executor received unknown [$unhandled] message.")
  }

  private def parseTupleAndSendGraph(record: T): List[Long] = {
    try{
      graphBuilder.getUpdates(record).map(update => sendGraphUpdate(update))
    }
    catch {
      case e:Exception => List()
    }
  }

  private def sendGraphUpdate(message: GraphUpdate): Long = {
    update += 1
    val path             = getWriter(message.srcId)
    val id               = getMessageIDForWriter(path)

    val sentMessage = TrackedGraphUpdate(s"$builderID", id, message)
    log.debug(s"Builder Executor sending message [$sentMessage] to PubSub")

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


  def broadcastBuilderTimeSync(time: Long) = {
    if(safe) {
        getAllWriters().foreach { workerPath =>
        mediator ! new DistributedPubSubMediator.Send(
          workerPath,
          BuilderTimeSync(time, s"$builderID", getMessageIDForWriter(workerPath))
        )
      }
    }
  }

}



object BuilderExecutor {
  object Message {
    case object StartUp
    case object TimeBroadcast
    case object KeepAlive
    case class DataFinishedSync(time:Long)
    case class BuilderTimeSync(msgTime:Long, BuilderId:String, builderTime:Int)
  }

  private case class State(
                            newestTime: Long,
                            dataFinished: Boolean,
                            restBuilderNewestFinishedTime: Long
  )
}
