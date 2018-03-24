package com.gwz.dockerexp.Actors.RaphtoryActors


import java.io.FileWriter
import java.util.Calendar

import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.Actor
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.gwz.dockerexp.caseclass.BenchmarkUpdater
import kamon.Kamon

import scala.concurrent.duration._
//import kafka.producer.KeyedMessage

import scala.io.Source
import scala.util.Random


class UpdateGen(managerCount:Int) extends RaphtoryActor{
  val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)
  var totalCount      = 50
  var currentMessage  = 0
  var previousMessage = 0
  var safe            = true

  if(!new java.io.File("CurrentMessageNumber.txt").exists) storeRunNumber(0) //check if there is previous run which has created messages, fi not create file
  else for (line <- Source.fromFile("CurrentMessageNumber.txt").getLines()) {currentMessage = line.toInt} //otherwise read previous number

  override def preStart() { //set up partition to report how many messages it has processed in the last X seconds
    println("Prestarting")
    context.system.scheduler.schedule(Duration(3, SECONDS), Duration(1, SECONDS), self,"random")
    context.system.scheduler.schedule(Duration(7, SECONDS), Duration(2, SECONDS), self,"benchmark")
  }

  //************* MESSAGE HANDLING BLOCK
  override def receive: Receive = {
    case "Safe" => {safe = true;}
    case "addVertex" => vertexAdd()
    case "removeVertex" => vertexRemove()
    case "addEdge" => edgeAdd()
    case "removeEdge" => edgeRemove()
    case "random" => {
      if(safe){
        running()
      }
    }
    case "benchmark" => benchmark()
    case _ => println("message not recognized!")
  }

  def running() : Unit = {
    genRandomCommands(totalCount)
    println(s"${Calendar.getInstance().getTime}:$totalCount")
    //totalCount+=1000
  }

  def benchmark():Unit={
    val diff = currentMessage - previousMessage
    kGauge.refine("actor" -> "Updater", "name" -> "diff").set(diff)
    previousMessage = currentMessage
  }

  def genRandomCommands(number:Int):Unit={
    var commandblock = ""
    for (i <- 0 until number){
      val random = Random.nextFloat()
      var command = ""
      if(random<=0.2) command =genVertexAdd()
      else if(random<=0.4) command = genVertexAdd()
      //else if(random<=0.5) command = genVertexRemoval()
      else if(random<=0.7) command = genEdgeAdd()
      else if(random<=0.8) command = genEdgeAdd()
      else                 command = genEdgeRemoval()

      mediator ! DistributedPubSubMediator.Send("/user/router",command,false)

      Kamon.counter("raphtory.updateGen.commandsSent").increment()
      //commandblock = s"$commandblock $command \n"
    }

  }

  def vertexAdd(){
    val command = genVertexAdd()
    sender ! command
    mediator ! DistributedPubSubMediator.Send("/user/router",command,false)
  }

  def vertexRemove(): Unit ={
    val command = genVertexRemoval()
    sender ! command
    mediator ! DistributedPubSubMediator.Send("/user/router",command,false)
  }

  def edgeAdd(): Unit ={
    val command = genEdgeAdd()
    sender ! command
    mediator ! DistributedPubSubMediator.Send("/user/router",command,false)
  }

  def edgeRemove(): Unit ={
    val command = genEdgeRemoval()
    sender ! genEdgeRemoval()
    mediator ! DistributedPubSubMediator.Send("/user/router",genEdgeRemoval(),false)
  }


  def storeRunNumber(runNumber:Int):Unit={
    val fw = new FileWriter(s"CurrentMessageNumber.txt") //if not create the file and show that we are starting at 0
    try {fw.write(runNumber.toString)}
    finally fw.close()
  }

  def genVertexAdd():String={
    currentMessage+=1
    s""" {"VertexAdd":{${getMessageID()}, ${genSrcID()}}}"""
  }
  def genVertexAdd(src:Int):String={ //overloaded method if you want to specify src ID
    currentMessage+=1
    s""" {"VertexAdd":{${getMessageID()}, ${genSrcID(src)}, ${genProperties(2,true)}}}"""
  }


  def genVertexUpdateProperties():String={
    currentMessage+=1
    s""" {"VertexUpdateProperties":{${getMessageID()}, ${genSrcID()}}}"""
  }
  def genVertexUpdateProperties(src:Int):String={ //overloaded to mass src
    currentMessage+=1
    s""" {"VertexUpdateProperties":{${getMessageID()}, ${genSrcID(src)}, ${genProperties(2,true)}}}"""
  }

  def genVertexRemoval():String={
    currentMessage+=1
    s""" {"VertexRemoval":{${getMessageID()}, ${genSrcID()}}}"""
  }

  def genVertexRemoval(src:Int):String={
    currentMessage+=1
    s""" {"VertexRemoval":{${getMessageID()}, ${genSrcID(src)}}}"""
  }

  def genEdgeAdd():String={
    currentMessage+=1
    s""" {"EdgeAdd":{${getMessageID()}, ${genSrcID()}, ${genDstID()}}}"""
  }
  def genEdgeAdd(src:Int,dst:Int):String={
    currentMessage+=1
    s""" {"EdgeAdd":{${getMessageID()}, ${genSrcID(src)}, ${genDstID(dst)}}}"""
  }

  def genEdgeUpdateProperties():String={
    currentMessage+=1
    s""" {"EdgeUpdateProperties":{${getMessageID()}, ${genSrcID()}, ${genDstID()}}}"""
  }
  def genEdgeUpdateProperties(src:Int,dst:Int):String={
    currentMessage+=1
    s""" {"EdgeUpdateProperties":{${getMessageID()}, ${genSrcID(src)}, ${genDstID(dst)}, ${genProperties(2,true)}}}"""
  }

  def genEdgeRemoval():String={
    currentMessage+=1
    s""" {"EdgeRemoval":{${getMessageID()}, ${genSrcID()}, ${genDstID()}}}"""
  }
  def genEdgeRemoval(src:Int,dst:Int):String={
    currentMessage+=1
    s""" {"EdgeRemoval":{${getMessageID()}, ${genSrcID(src)}, ${genDstID(dst)}}}"""
  }


  def genSetSrcID():String = s""" "srcID":9 """
  def genSetDstID():String = s""" "dstID":10 """
  def genSrcID():String = s""" "srcID":${Random.nextInt(100000)} """
  def genDstID():String = s""" "dstID":${Random.nextInt(100000)} """
  def genSrcID(src:Int):String = s""" "srcID":$src """
  def genDstID(dst:Int):String = s""" "dstID":$dst """

  def getMessageID():String = s""" "messageID":$currentMessage """

  def genProperties(numOfProps:Int,randomProps:Boolean):String ={
    var properties = "\"properties\":{"
    for(i <- 1 to numOfProps){
      val propnum = {if(randomProps) Random.nextInt(30) else i}
      if(i<numOfProps) properties = properties + s""" "property$propnum":${Random.nextInt()}, """
      else properties = properties + s""" "property$propnum":${Random.nextInt()} }"""
    }
    properties
  }

  def getManager(srcId:Int):String = s"/user/Manager_${srcId % managerCount}" //simple srcID hash at the moment

}
