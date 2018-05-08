package com.raphtory.core.actors.partitionmanager

import akka.actor.ActorRef

import scala.util.{Failure, Success}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.model.communication._
import com.raphtory.core.storage.controller.GraphRepoProxy
import com.raphtory.core.utils.Utils
import monix.execution.{ExecutionModel, Scheduler}

import scala.concurrent.Future

class PartitionReader(id : Int, test : Boolean, managerCountVal : Int) extends RaphtoryActor {
  implicit var managerCount : Int = managerCountVal
  val managerID    : Int = id                   //ID which refers to the partitions position in the graph manager map
  implicit val s : Scheduler = Scheduler(ExecutionModel.BatchedExecution(1024))
  val mediator : ActorRef   = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages

  mediator ! DistributedPubSubMediator.Put(self)
  mediator ! DistributedPubSubMediator.Subscribe(Utils.readersTopic, self)
  implicit val proxy = GraphRepoProxy
  override def preStart() = {
    println("Starting reader")
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

        println("Inside future")
        val value = analyzer.analyse()

            println("StepEnd success. Sending to " + senderPath.toStringWithoutAddress)
            println(value)
            mediator ! DistributedPubSubMediator.Send(senderPath.toStringWithoutAddress, EndStep(value), false)
    }
    case GetNetworkSize() =>
      sender() ! NetworkSize(GraphRepoProxy.getVerticesSet().size)
    case UpdatedCounter(newValue) => {
      managerCount = newValue
    }
    case e => println(s"[READER] not handled message " + e)
  }
}
