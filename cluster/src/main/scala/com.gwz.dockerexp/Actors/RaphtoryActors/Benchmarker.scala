package com.gwz.dockerexp.Actors.RaphtoryActors

/**
  * Created by Mirate on 11/07/2017.
  */
import java.util.Calendar

import akka.actor.{Actor, ActorRef}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.gwz.dockerexp.caseclass.BenchmarkUpdate



class Benchmarker(managerCount:Int) extends Actor{
  val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)


  override def receive: Receive = {
    case BenchmarkUpdate(id,count) => update(id,count)
    case _ => println("message not recognized!")
  }


  def update(id:Int,count:Int) ={
      val now = Calendar.getInstance().getTime()
      println(s"$now: Partition manager $id has processed $count messages")
  }

  def getManager(srcId:Int):String = s"/user/Manager_${srcId % managerCount}" //simple srcID hash at the moment

}
