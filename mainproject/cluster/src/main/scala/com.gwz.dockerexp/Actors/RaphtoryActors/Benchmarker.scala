package com.gwz.dockerexp.Actors.RaphtoryActors

/**
  * Created by Mirate on 11/07/2017.
  */
import java.util.Calendar

import akka.actor.Actor
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.gwz.dockerexp.caseclass.{BenchmarkPartitionManager, BenchmarkRouter, BenchmarkUpdater}

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._



class Benchmarker(managerCount:Int) extends RaphtoryActor{
  val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)
  //var blockMap = Map[Int,BenchmarkBlock]()
  var currentCount:Int = 0
  var partitionlist = List[Int]()
  var partitionlist2 = List[Int]()
  var updaterlist = List[Int]()
  var routerlist = List[Int]()

  override def preStart() {
    context.system.scheduler.schedule(Duration(1, SECONDS),Duration(10, SECONDS),self,"tick")
  }

  override def receive: Receive = {
    case "tick" => {
      csvprint()
    }
    case BenchmarkPartitionManager(id,count,secondCount) => {
      partitionlist = partitionlist.+:(count)
      partitionlist2 = partitionlist2.+:(secondCount)
      //println(s"Received partition update ${Calendar.getInstance().getTime}: $count")
    }
    case BenchmarkUpdater(count) => {
      updaterlist = updaterlist.+:(count)
      //println(s"Received updater count ${Calendar.getInstance().getTime}: $count")
    }
    case BenchmarkRouter(count) => {
      routerlist = routerlist.+:(count)
      //println(s"Received router count ${Calendar.getInstance().getTime}: $count")
    }

    case _ => println("message not recognized!")
  }

  def csvprint():Unit={
    println(s"${Calendar.getInstance().getTime},${updaterlist.sum},${routerlist.sum},${partitionlist.sum},${partitionlist.sum+partitionlist2.sum}")
    partitionlist = List[Int]()
    partitionlist2 = List[Int]()
    updaterlist = List[Int]()
    routerlist = List[Int]()
  }

  def prettyprint():Unit={
    println(s"Total count at ${Calendar.getInstance().getTime}: \n" +
      s"    The Updater has generated ${updaterlist.sum} messages \n" +
      s"    The Graph Routers have processed ${routerlist.sum} messages \n" +
      s"    The partition managers have processed ${partitionlist.sum} messages \n" +
      s"    The partition managers have processed ${partitionlist2.sum} secondary messages \n")
    partitionlist = List[Int]()
    partitionlist2 = List[Int]()
    updaterlist = List[Int]()
    routerlist = List[Int]()
  }


  def getManager(srcId:Int):String = s"/user/Manager_${srcId % managerCount}" //simple srcID hash at the moment

}

