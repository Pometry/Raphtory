package com.gwz.dockerexp.Actors.RaphtoryActors

/**
  * Created by Mirate on 11/07/2017.
  */
import java.util.Calendar

import akka.actor.Actor
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.gwz.dockerexp.caseclass.BenchmarkUpdate

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._



class Benchmarker(managerCount:Int) extends Actor{
  val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)
  //var blockMap = Map[Int,BenchmarkBlock]()
  var currentCount:Int = 0

  override def preStart() {
    context.system.scheduler.schedule(Duration(1, SECONDS),Duration(10, SECONDS),self,"tick")
  }

  override def receive: Receive = {
    case "tick" => {println(s"Total count at ${Calendar.getInstance().getTime}: $currentCount"); currentCount=0}
    case BenchmarkUpdate(id,blockID,count) => currentCount=currentCount+count
    case _ => println("message not recognized!")
  }




  def getManager(srcId:Int):String = s"/user/Manager_${srcId % managerCount}" //simple srcID hash at the moment

  /*class BenchmarkBlock(managerCount:Int,blockID:Int){
    var totalCount:Int =0
    var messageCount:Int =0

    var initialTime:Date = new Date()
    var initialManager:Int =0
    var endTime:Date = new Date()
    var endManager:Int = 0

    def insert(count:Int,managerID:Int)={
      if(messageCount==0) {
        initialTime=Calendar.getInstance().getTime
        initialManager=managerID
      }
      totalCount = totalCount +count
      messageCount = messageCount +1
      if(messageCount==managerCount){
        endManager=managerID
        endTime=Calendar.getInstance().getTime
        println(s"Total count for block $blockID: $totalCount")
        println(s"Initial message received from $initialManager at $initialTime")
        println(s"End message received from $endManager at $endTime")
      }
def update(managerid:Int,blockID:Int,count:Int) ={
      //if(blockMap.contains(blockID)) blockMap(blockID).insert(count,managerid)
      //else {
      //  blockMap = blockMap.updated(blockID,new BenchmarkBlock(managerCount,blockID))
      //  blockMap(blockID).insert(count,managerid)
      //}
  }
    }
  }*/

}

