package com.raphtory.core.actors.ClusterManagement

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.actors.ClusterManagement.WatermarkManager.Message._
import com.raphtory.core.actors.RaphtoryActor
import kamon.Kamon

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import scala.concurrent.duration._
import scala.collection.parallel.mutable.ParTrieMap
import scala.concurrent.ExecutionContext

class WatermarkManager(managerCount: Int) extends RaphtoryActor  {
  implicit val executionContext: ExecutionContext = context.system.dispatcher

  override def preStart(): Unit = {
    context.system.scheduler.scheduleOnce(delay = 60.seconds, receiver = self, message = "probe")
  }
  val safeTime = Kamon.gauge("Raphtory_Safe_Time").withTag("actor",s"WatermarkManager")

  private val safeMessageMap = ParTrieMap[String, Long]()
  var counter = 0;

  val mediator: ActorRef = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)

  override def receive: Receive = {
    case "probe" => probeWatermark()
    case u:WatermarkTime => processWatermarkTime(u)
  }

  def probeWatermark() = {
    val workerPaths = for {
      i <- 0 until managerCount
      j <- 0 until totalWorkers
    } yield s"/user/Manager_${i}_child_$j"

    workerPaths.foreach { workerPath =>
      mediator ! new DistributedPubSubMediator.Send(
        workerPath,
        ProbeWatermark
      )
    }
  }

  def processWatermarkTime(u:WatermarkTime):Unit = {
    safeMessageMap put(sender().path.toString,u.time)
    counter +=1
    if(counter%(totalWorkers*managerCount)==0) {
      val watermark = safeMessageMap.map(x=>x._2).min
      safeTime.update(watermark)
      val max = safeMessageMap.maxBy(x=> x._2)
      val min = safeMessageMap.minBy(x=> x._2)
      val t = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"))
      println(s"$t . Minimum Watermark: ${min._1} ${min._2} Maximum Watermark: ${max._1} ${max._2}")
//      println(s". Minimum Watermark: ${min._1} ${min._2} Maximum Watermark: ${max._1} ${max._2}")
      context.system.scheduler.scheduleOnce(delay = 10.seconds, receiver = self, message = "probe")
    }
  }
}

object WatermarkManager {
  object Message {
    case object ProbeWatermark
    case class WatermarkTime(time:Long)
  }
}