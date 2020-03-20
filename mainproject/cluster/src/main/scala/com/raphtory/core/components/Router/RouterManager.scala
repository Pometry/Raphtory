package com.raphtory.core.components.Router

import akka.actor.{Actor, ActorRef, Props}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.model.communication._
import com.raphtory.core.utils.Utils.getManager
import kamon.Kamon

import scala.collection.parallel.mutable.ParTrieMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, SECONDS}

class RouterManager(val routerId:Int, val initialManagerCount:Int, slaveType:String) extends Actor {
  val kGauge         = Kamon.gauge("raphtory.benchmarker")
  val kCounter       = Kamon.counter("raphtory.counters")

  private val children = 10
  private var childMap              : ParTrieMap[Int,ActorRef] = ParTrieMap[Int,ActorRef]()

  private   var       managerCount : Int = initialManagerCount
  protected final def getManagerCount = managerCount
  private var count = 0

  protected final val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)

  override def preStart() {
    context.system.scheduler.schedule(Duration(10, SECONDS), Duration(1, SECONDS),self,"tick")
    context.system.scheduler.schedule(Duration(10, SECONDS), Duration(10, SECONDS), self, "keep_alive")
    for(i <- 0 until children){
      childMap.put(i,context.actorOf(Props(Class.forName(slaveType),routerId,initialManagerCount),s"child_$i"))
    }
  }
  override def receive: Receive = {
    case "tick" => tick()
    case "keep_alive" => keepAlive()
    case UpdatedCounter(newValue) => {newPmJoined(newValue);childMap.values.foreach(a => a ! UpdatedCounter(newValue))} //inform all children
    case e : Any => allocateRecord(e);
  }
  protected def allocateRecord(record:Any):Unit = {
    recordUpdate()
    childMap get(count%children) match {
      case Some(child) => child ! AllocateJob(record)
    }
  }

  private def recordUpdate() = {
    count += 1
    kCounter.refine("actor" -> "Router", "name" -> "count").increment()
    Kamon.gauge("raphtory.router.countGauge").set(count)
  }

  def toPartitionManager[T <: GraphUpdate](message:T): Unit = mediator ! DistributedPubSubMediator.Send(getManager(message.srcID, getManagerCount), message , false)
  private def newPmJoined(newValue : Int) = if (managerCount < newValue) managerCount = newValue
  private def keepAlive()                 = mediator ! DistributedPubSubMediator.Send("/user/WatchDog", RouterUp(routerId), false)
  private def tick()                      = {kGauge.refine("actor" -> "Router", "name" -> "count").set(count);count = 0}
}
