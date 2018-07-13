package com.raphtory.core.actors.datasource

import com.raphtory.core.model.communication._
import com.raphtory.core.actors.RaphtoryActor

import akka.actor._
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import akka.pattern.ask
import akka.util.Timeout

import kamon.Kamon

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

class UpdateGen extends RaphtoryActor with Timers {

  val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)
  var totalCount      = 100
  var freq            = System.getenv().getOrDefault("UPDATES_FREQ", "1000").toInt    // (Updates/s) - Hz
  var increase        = System.getenv().getOrDefault("RAMP_FLAG", "false").toBoolean  // (Updates/s) - Hz

  var currentMessage  = 0
  var previousMessage = 0
  var safe            = false
  var counter         = 0

  def getPeriodDuration(unit : TimeUnit) : FiniteDuration = {
    val period : Long = unit.convert(1, SECONDS)/freq
    println(period)
    val x = Duration(period, unit)
    println(x)
    x
  }

  override def preStart() { //set up partition to report how many messages it has processed in the last X seconds
    println(s"Prestarting ($freq Hz)")
    context.system.scheduler.schedule(Duration(1, MINUTES), Duration(1, MILLISECONDS), self, "random")
    context.system.scheduler.schedule(Duration(7, SECONDS), Duration(1, SECONDS), self,"benchmark")
    context.system.scheduler.schedule(Duration(7, SECONDS), Duration(1, SECONDS), self,"stateCheck")
    context.system.scheduler.schedule(Duration(5, MINUTES), Duration(5, MINUTES), self, "increase")
  }

  //************* MESSAGE HANDLING BLOCK
  override def receive: Receive = {
    case "stateCheck" => checkUp()
    case "addVertex" => vertexAdd()
    case "removeVertex" => vertexRemove()
    case "addEdge" => edgeAdd()
    case "removeEdge" => edgeRemove()
    case "increase" =>
      if (increase)
        freq += 1000
    case "random" => {
      if(safe) {
        genRandomCommands(freq/1000)
      }
    }
    case "benchmark" => benchmark()
    case _ => println("message not recognized!")
  }

  def running() : Unit = {
    genRandomCommands(totalCount)
    //totalCount+=1000
  }

  def benchmark():Unit={
    val diff = currentMessage - previousMessage
    previousMessage = currentMessage
    counter = 0
    kGauge.refine("actor" -> "Updater", "name" -> "diff").set(diff)
  }

  def checkUp():Unit={
    if(!safe){
      try{
        implicit val timeout: Timeout = Timeout(10 seconds)
        val future = mediator ? DistributedPubSubMediator.Send("/user/WatchDog", ClusterStatusRequest, false)
        safe = Await.result(future, timeout.duration).asInstanceOf[ClusterStatusResponse].clusterUp
      }
      catch {
        case e: java.util.concurrent.TimeoutException => {
          safe = false
        }
      }
    }
  }

  def distribution() : String = {
    val random = Random.nextFloat()
    if (random <= 0.3)      genVertexAdd()
    else if (random <= 0.7) genEdgeAdd()
    else if (random <= 0.8) genVertexRemoval()
    else                    genEdgeRemoval()
  }

  /**
    *
    * @param number: frequency in mHz
    */
  def genRandomCommands(number : Int) : Unit = {
    (1 to number) foreach (_ => {
      val command = distribution()
      counter += 1
      mediator ! DistributedPubSubMediator.Send("/user/router", command, false)
      Kamon.counter("raphtory.updateGen.commandsSent").increment()
      kGauge.refine("actor" -> "Updater", "name" -> "updatesSentGauge").set(counter)
    })
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
    s""" {"VertexUpdateProperties":{${getMessageID()}, ${genSrcID(1)}}}"""
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

  def getMessageID():String = s""" "messageID":${System.currentTimeMillis()} """

  def genProperties(numOfProps:Int,randomProps:Boolean):String ={
    var properties = "\"properties\":{"
    for(i <- 1 to numOfProps){
      val propnum = {if(randomProps) Random.nextInt(30) else i}
      if(i<numOfProps) properties = properties + s""" "property$propnum":${Random.nextInt()}, """
      else properties = properties + s""" "property$propnum":${Random.nextInt()} }"""
    }
    properties
  }
}
