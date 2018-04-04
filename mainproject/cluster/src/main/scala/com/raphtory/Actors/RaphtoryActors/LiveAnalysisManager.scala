package com.raphtory.Actors.RaphtoryActors

import scala.concurrent.ExecutionContext.Implicits.global
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.Actors.RaphtoryActors.Analaysis.TestAnalyser
import com.raphtory.caseclass.{LiveAnalysis, PartitionsCount, Results}
import com.raphtory.utils.Utils

import scala.concurrent.duration._



class LiveAnalysisManager(name:String) extends RaphtoryActor{
  val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)
  mediator ! DistributedPubSubMediator.Subscribe(Utils.partitionsTopic, self)
  var managerCount : Int = 0

  override def preStart() { //set up partition to report how many messages it has processed in the last X seconds
    println("Prestarting")
    context.system.scheduler.schedule(Duration(10, SECONDS),Duration(10, SECONDS),self,"analyse")
  }

  //************* MESSAGE HANDLING BLOCK
  override def receive: Receive = {
    case "analyse" => analyse()
    case Results(result) => println(result)
    case PartitionsCount(newValue) => { // TODO redundant in Router and LAM (https://stackoverflow.com/questions/37596888/scala-akka-implement-abstract-class-with-subtype-parameter)
      managerCount = newValue
      println(s"Maybe a new PartitionManager has arrived: ${newValue}")
    }
  }

  def analyse() ={
      for(i <- 0 until managerCount){
        mediator ! DistributedPubSubMediator.Send(s"/user/Manager_$i",LiveAnalysis(name,new TestAnalyser),false)
      }
  }

}

