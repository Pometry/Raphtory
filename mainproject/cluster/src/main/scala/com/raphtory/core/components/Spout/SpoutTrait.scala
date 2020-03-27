package com.raphtory.core.components.Spout

import akka.actor.Actor
import akka.actor.Timers
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import akka.pattern.ask
import akka.util.Timeout
import com.raphtory.core.model.communication._
import kamon.Kamon

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.language.postfixOps

trait SpoutTrait extends Actor with Timers {
  private var currentMessage  = 0
  private var previousMessage = 0
  private var safe            = false
  private var counter         = 0
  case class StartSpout()
  val kGauge   = Kamon.gauge("raphtory.benchmarker")
  val kCounter = Kamon.counter("raphtory.counters")

  final protected val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)
  val debug = System.getenv().getOrDefault("DEBUG", "false").trim.toBoolean
  override def preStart() {
    context.system.scheduler.schedule(Duration(7, SECONDS), Duration(1, SECONDS), self, "benchmark")
    context.system.scheduler.schedule(Duration(7, SECONDS), Duration(1, SECONDS), self, "stateCheck")
    context.system.scheduler.scheduleOnce(Duration(1, SECONDS), self, "isSafe")
  }

  def AllocateSpoutTask(duration: Duration, task: Any) =
    context.system.scheduler.scheduleOnce(Duration(duration._1, duration._2), self, task)

  protected def recordUpdate(): Unit = {
    if (debug) println("sent update")
    counter += 1
    currentMessage += 1
    Kamon.counter("raphtory.updateGen.commandsSent").increment()
    kGauge.refine("actor" -> "Updater", "name" -> "updatesSentGauge").set(counter)
  }

  protected def sendTuple(command: String): Unit = {
    recordUpdate()
    mediator ! DistributedPubSubMediator.Send(s"/user/router", command /*Command(command, value)*/, false)
  }

  protected def sendTuple[T <: SpoutGoing](command: T): Unit = {
    recordUpdate()
    mediator ! DistributedPubSubMediator.Send("/user/router", command, false)
  }

  protected def ProcessSpoutTask(rcvdMessage: Any)

  private def isSafe() =
    if (safe)
      context.system.scheduler.scheduleOnce(Duration(1, MILLISECONDS), self, StartSpout)
    else
      context.system.scheduler.scheduleOnce(Duration(1, SECONDS), self, "isSafe")
  def start() = safe = true
  def stop()  = safe = false

  final override def receive: Receive = {
    case "stateCheck" => checkUp()
    case "benchmark"  => benchmark()
    case "isSafe"     => isSafe()
    case other: Any   => ProcessSpoutTask(other)
  }

  private def benchmark(): Unit = {
    val diff = currentMessage - previousMessage
    previousMessage = currentMessage
    counter = 0
    kGauge.refine("actor" -> "Updater", "name" -> "diff").set(diff)
  }

  private def checkUp(): Unit =
    if (!safe)
      try {
        implicit val timeout: Timeout = Timeout(10 seconds)
        val future                    = mediator ? DistributedPubSubMediator.Send("/user/WatchDog", ClusterStatusRequest, false)
        safe = Await.result(future, timeout.duration).asInstanceOf[ClusterStatusResponse].clusterUp
      } catch {
        case e: java.util.concurrent.TimeoutException =>
          safe = false
      }
}
