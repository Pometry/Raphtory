package com.raphtory.core.components.orchestration.raphtoryleader

import java.util.concurrent.atomic.AtomicLong
import akka.actor.ActorRef
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.components.RaphtoryActor
import com.raphtory.core.components.RaphtoryActor.totalPartitions
import com.raphtory.core.components.orchestration.raphtoryleader.WatermarkManager.Message.{ProbeWatermark, WatermarkTime, WhatsTheTime}

import java.time.LocalDateTime
import scala.collection.parallel.mutable.ParTrieMap
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class WatermarkManager extends RaphtoryActor  {
  implicit val executionContext: ExecutionContext = context.system.dispatcher

  override def preStart(): Unit = {
    context.system.scheduler.scheduleOnce(delay = 60.seconds, receiver = self, message = "probe")
  }
  val safeTimestamp:AtomicLong = new AtomicLong(0)

  private val safeMessageMap = ParTrieMap[String, Long]()
  var counter = 0;

  val mediator: ActorRef = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)

  override def receive: Receive = {
    case "probe" => probeWatermark()
    case u:WatermarkTime => processWatermarkTime(u)
    case WhatsTheTime => sender() ! WatermarkTime(safeTimestamp.get())
  }

  def probeWatermark() = {
    getAllWriters().foreach { workerPath =>
      mediator ! new DistributedPubSubMediator.Send(
        workerPath,
        ProbeWatermark
      )
    }
  }

  def processWatermarkTime(u:WatermarkTime):Unit = {
    safeMessageMap put(sender().path.toString,u.time)
    counter +=1
    if(counter==totalPartitions) {
      safeTimestamp.set(safeMessageMap.map(x=>x._2).min)

      val max = safeMessageMap.maxBy(x=> x._2)
      val min = safeMessageMap.minBy(x=> x._2)
      log.info(s"Minimum Watermark: ${min._1} ${min._2} Maximum Watermark: ${max._1} ${max._2}")
      context.system.scheduler.scheduleOnce(delay = 10.seconds, receiver = self, message = "probe")
      counter=0
    }
  }
}

object WatermarkManager {
  object Message {
    case object ProbeWatermark
    case class WatermarkTime(time:Long)
    case object SaveState
    case object WhatsTheTime
  }
}