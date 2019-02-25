package com.raphtory.core.actors.router

import com.raphtory.core.model.communication._
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.utils.Utils.getManager
import monix.execution.{ExecutionModel, Scheduler}
import kamon.Kamon
import monix.eval.Task

import scala.concurrent.duration.{Duration, SECONDS}

trait RouterTrait extends RaphtoryActor {

  // To be overrided in the routers
  protected def routerId : Int
  protected def initialManagerCount : Int
  protected def otherMessages(rcvdMessage : Any)
  protected final def getManagerCount = managerCount

  protected final val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)

  implicit val s : Scheduler = Scheduler(ExecutionModel.BatchedExecution(1024))
  println(akka.serialization.Serialization.serializedActorPath(self))

  private var count = 0
  private var managerCount : Int = initialManagerCount
  override def preStart() {
    context.system.scheduler.schedule(Duration(7, SECONDS),
      Duration(1, SECONDS),self,"tick")
    context.system.scheduler.schedule(Duration(8, SECONDS),
      Duration(10, SECONDS), self, "keep_alive")
  }

  private def keepAlive() = mediator ! DistributedPubSubMediator.Send("/user/WatchDog", RouterUp(routerId), false)

  private def tick() = {
    kGauge.refine("actor" -> "Router", "name" -> "count").set(count)
    count = 0
  }

  protected def parseJSON(command : String) = {
    recordUpdate()
  }

  override def receive: Receive = {
    case "tick" => tick()
    case "keep_alive" => keepAlive()
    case UpdatedCounter(newValue) => newPmJoined(newValue)
    case command:String =>  {
      Task.eval(this.parseJSON(command)).fork.runAsync
    }
    case e : Any => otherMessages(e)
  }

  protected def recordUpdate(): Unit ={
    count += 1
    kCounter.refine("actor" -> "Router", "name" -> "count").increment()
    Kamon.gauge("raphtory.router.countGauge").set(count)
  }

  private def newPmJoined(newValue : Int) = if (managerCount < newValue) {
      managerCount = newValue
  }

  def toPartitionManager[T <: RaphWriteClass](message:T): Unit ={
    //val manager = getManager(message.srcId, getManagerCount)
    //val man = manager.charAt(manager.length-1).toString.toInt
    //checkList(man) = checkList(man) +1
    mediator ! DistributedPubSubMediator.Send(getManager(message.srcId, getManagerCount), message , false)
  }

}


/*def vertexAdd(command : JsObject) : Unit
def vertexUpdateProperties(command : JsObject) : Unit
def vertexRemoval(command : JsObject) : Unit

def edgeAdd(command : JsObject) : Unit
def edgeUpdateProperties(command : JsObject) : Unit
def edgeRemoval(command : JsObject) : Unit*/
