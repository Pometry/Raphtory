package com.raphtory.core.components.ClusterManagement

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.model.communication.{UpdateArrivalTime, WatermarkTime}
import kamon.Kamon

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap

case class queueItem(wallclock:Long,timestamp:Long)extends Ordered[queueItem] {
  def compare(that: queueItem): Int = (that.timestamp-this.timestamp).toInt
}

class WatermarkManager(managerCount: Int) extends Actor with ActorLogging  {

  val spoutWallClock = Kamon.histogram("Raphtory_Wall_Clock").withTag("Actor","Watchdog")
  val watermarkqueue = mutable.PriorityQueue[queueItem]()
  private val safeMessageMap = ParTrieMap[String, Long]()
  var counter = 0;
  val mediator: ActorRef = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)
  override def receive: Receive = {
    case u:UpdateArrivalTime => processUpdateArrivalTime(u)
  }

  def processUpdateArrivalTime(u: UpdateArrivalTime):Unit = watermarkqueue += queueItem(u.wallClock,u.time)

  def processWatermarkTime(u:WatermarkTime):Unit = {
    val currentTime = System.currentTimeMillis()
    safeMessageMap put(sender().toString(),u.time)
    counter +=1
    if(counter%(10*managerCount)==0) {
      val watermark = safeMessageMap.values.min
      while((watermarkqueue nonEmpty) && (watermarkqueue.head.timestamp<= watermark)) {
        spoutWallClock.record(currentTime-watermarkqueue.dequeue().wallclock)
      }
    }
  }
}
