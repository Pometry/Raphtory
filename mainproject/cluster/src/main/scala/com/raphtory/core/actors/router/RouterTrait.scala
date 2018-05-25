package com.raphtory.core.actors.router

import com.raphtory.core.model.communication._
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.actors.RaphtoryActor
import monix.execution.{ExecutionModel, Scheduler}
import kamon.Kamon
import monix.eval.Task

import scala.concurrent.duration.{Duration, SECONDS}

trait RouterTrait extends RaphtoryActor {

  // To be overrided in the routers
  protected def routerId : Int
  protected def initialManagerCount : Int
  protected def otherMessages(rcvdMessage : Any)

  // Let's call the super.parseJSON in the Router implementation to get Kamon Metrics
  protected def parseJSON(command : String) = {
    count += 1
    kCounter.refine("actor" -> "Router", "name" -> "count").increment()
    Kamon.gauge("raphtory.router.countGauge").set(count)
  }

  /*protected def parseCommand(comm : Command) = {
    count += 1
    kCounter.refine("actor" -> "Router", "name" -> "count").increment()
    Kamon.gauge("raphtory.router.countGauge").set(count)
  }*/

  override def preStart() {
    context.system.scheduler.schedule(Duration(7, SECONDS),
      Duration(1, SECONDS),self,"tick")
    context.system.scheduler.schedule(Duration(8, SECONDS),
      Duration(10, SECONDS), self, "keep_alive")
  }


  final override def receive: Receive = {
    case "tick" => tick()
    case "keep_alive" => keepAlive()
    case UpdatedCounter(newValue) => newPmJoined(newValue)
    case command:String =>  {
      Task.eval(this.parseJSON(command)).fork.runAsync
    }
    //case x : Command => Task.eval(this.parseCommand(x)).fork.runAsync
    case e => otherMessages(e)
  }

  protected final def getManagerCount = managerCount

  protected final val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)


  private var count = 0
  private var managerCount : Int = initialManagerCount

  private def keepAlive() = mediator ! DistributedPubSubMediator.Send("/user/WatchDog", RouterUp(routerId), false)

  private def tick() = {
    kGauge.refine("actor" -> "Router", "name" -> "count").set(count)
    count = 0
  }

  private def newPmJoined(newValue : Int) = if (managerCount < newValue) {
      managerCount = newValue
  }

  implicit val s : Scheduler = Scheduler(ExecutionModel.BatchedExecution(1024))
  println(akka.serialization.Serialization.serializedActorPath(self))
}
/*def vertexAdd(command : JsObject) : Unit
def vertexUpdateProperties(command : JsObject) : Unit
def vertexRemoval(command : JsObject) : Unit

def edgeAdd(command : JsObject) : Unit
def edgeUpdateProperties(command : JsObject) : Unit
def edgeRemoval(command : JsObject) : Unit*/
