package com.raphtory.core.actors.ClusterManagement

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.model.communication.WatermarkTime
import kamon.Kamon

import scala.collection.parallel.mutable.ParTrieMap

class WatermarkManager(managerCount: Int) extends RaphtoryActor  {

  val safeTime = Kamon.gauge("Raphtory_Safe_Time").withTag("actor",s"WatermarkManager")

  private val safeMessageMap = ParTrieMap[String, Long]()
  var counter = 0;

  val mediator: ActorRef = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)

  override def receive: Receive = {
    case u:WatermarkTime => processWatermarkTime(u)
  }


  def processWatermarkTime(u:WatermarkTime):Unit = {
    safeMessageMap put(sender().toString(),u.time)
    counter +=1
    if(counter%(totalWorkers*managerCount)==0) {
      val watermark = safeMessageMap.map(x=>x._2).min
      safeTime.update(watermark)
    }
  }
}
