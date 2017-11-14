package com.gwz.dockerexp.Actors.ClusterActors

/**
  * Created by Mirate on 11/07/2017.
  */
import java.util.{Calendar, Date}

import akka.actor.{Actor, ActorRef}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.gwz.dockerexp.caseclass.{BenchmarkUpdate, LiveAnalysis}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class ClusterUpActor(managerCount:Int) extends Actor{
  val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)


  override def preStart() {
    context.system.scheduler.schedule(Duration(2, SECONDS),Duration(1, SECONDS),self,"tick")
  }

  override def receive: Receive = {
    case "tick" => {mediator ! DistributedPubSubMediator.Send("/user/updateGen",LiveAnalysis(),false); println("got")}
  }

  def getManager(srcId:Int):String = s"/user/Manager_${srcId % managerCount}" //simple srcID hash at the moment

}

