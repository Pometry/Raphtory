package com.gwz.dockerexp.Actors.RaphtoryActors

import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.Actor
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.gwz.dockerexp.Actors.RaphtoryActors.Analaysis.{OpenAnalyser, Test2Analyser}
import com.gwz.dockerexp.GraphEntities.{Edge, Vertex}
import com.gwz.dockerexp.caseclass.{LiveAnalysis, OpenLiveAnalysis, Results}

import scala.concurrent.duration._



class LiveAnalysisManager(managerCount:Int,name:String) extends Actor{
  val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)

  override def preStart() { //set up partition to report how many messages it has processed in the last X seconds
    println("Prestarting")
    context.system.scheduler.schedule(Duration(10, SECONDS),Duration(10, SECONDS),self,"analyse")
  }

  //************* MESSAGE HANDLING BLOCK
  override def receive: Receive = {
    case "analyse" => analyse()
    case Results(result) => println(result)
  }

  def analyse() ={
      for(i <- 0 until managerCount){
        mediator ! DistributedPubSubMediator.Send(s"/user/Manager_$i",LiveAnalysis(name,new Test2Analyser),false)
      }
  }
  def analyseRedux() ={
    for(i <- 0 until managerCount){
      mediator ! DistributedPubSubMediator.Send(s"/user/Manager_$i",OpenLiveAnalysis(name,new OpenAnalyser(analyse)),false)
    }
  }
  def analyse(vertices:Map[Int,Vertex],edges:Map[(Int,Int),Edge]):Object = "Please Work"


}

