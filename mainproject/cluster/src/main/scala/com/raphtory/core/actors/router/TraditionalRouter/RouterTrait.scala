package com.raphtory.core.actors.router.TraditionalRouter

import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import scala.concurrent.ExecutionContext.Implicits.global
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.model.communication._
import com.raphtory.core.utils.Utils.getManager
import kamon.Kamon

import scala.concurrent.duration.{Duration, SECONDS}

trait RouterTrait extends RaphtoryActor {

  // To be overrided in the routers
  protected def       routerId : Int
  protected def       initialManagerCount : Int
  protected def       otherMessages(rcvdMessage : Any)

  private   var       managerCount : Int = initialManagerCount
  protected final def getManagerCount = managerCount
  private var count = 0

  protected final val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)

  override def preStart() {
    context.system.scheduler.schedule(Duration(7, SECONDS),
      Duration(1, SECONDS),self,"tick")
    context.system.scheduler.schedule(Duration(8, SECONDS),
      Duration(10, SECONDS), self, "keep_alive")
  }

  override def receive: Receive = {
    case "tick" => tick()
    case "keep_alive" => keepAlive()
    case UpdatedCounter(newValue) => newPmJoined(newValue)
    case command:String =>  this.parseRecord(command)
    case e : Any => otherMessages(e)
  }

  protected def parseRecord(command : String) = {
    count += 1
    kCounter.refine("actor" -> "Router", "name" -> "count").increment()
    Kamon.gauge("raphtory.router.countGauge").set(count)
  }



  def toPartitionManager[T <: RaphWriteClass](message:T): Unit = mediator ! DistributedPubSubMediator.Send(getManager(message.srcId, getManagerCount), message , false)

  private def newPmJoined(newValue : Int) = if (managerCount < newValue) managerCount = newValue
  private def keepAlive()                 = mediator ! DistributedPubSubMediator.Send("/user/WatchDog", RouterUp(routerId), false)
  private def tick()                      = {kGauge.refine("actor" -> "Router", "name" -> "count").set(count);count = 0}
}
