package com.raphtory.core.actors.partitionmanager

import akka.actor.{ActorPath, ActorRef}

import scala.util.{Failure, Success}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.analysis.Analyser
import com.raphtory.core.model.communication._
import com.raphtory.core.storage.controller.GraphRepoProxy
import com.raphtory.core.utils.Utils
import monix.eval.Task
import monix.execution.ExecutionModel.AlwaysAsyncExecution
import monix.execution.{ExecutionModel, Scheduler}

class PartitionReader(id : Int, test : Boolean, managerCountVal : Int) extends RaphtoryActor {
  implicit var managerCount : Int = managerCountVal
  val managerID    : Int = id                   //ID which refers to the partitions position in the graph manager map
  val mediator : ActorRef   = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages
  implicit val s = Scheduler.computation()
  mediator ! DistributedPubSubMediator.Put(self)
  mediator ! DistributedPubSubMediator.Subscribe(Utils.readersTopic, self)
  implicit val proxy = GraphRepoProxy
  override def preStart() = {
    println("Starting reader")
  }

  private def analyze(analyzer : Analyser, senderPath : ActorPath) = {
    val value = analyzer.analyse()
    println("StepEnd success. Sending to " + senderPath.toStringWithoutAddress)
    println(value)
    mediator ! DistributedPubSubMediator.Send(senderPath.toStringWithoutAddress, EndStep(value), false)
  }
  override def receive : Receive = {
    case Setup(analyzer) => {
      println("Setup analyzer, sending Ready packet")
      analyzer.sysSetup()
      analyzer.setup()
      sender() ! Ready()
    }
    case NextStep(analyzer) => {
      println(s"Received new step for pm_$managerID")
      analyzer.sysSetup()
      val senderPath = sender().path
      Task.eval(this.analyze(analyzer, senderPath)).runAsync
    }
    case GetNetworkSize() =>
      sender() ! NetworkSize(GraphRepoProxy.getVerticesSet().size)
    case UpdatedCounter(newValue) => {
      managerCount = newValue
    }
    case e => println(s"[READER] not handled message " + e)
  }
}
