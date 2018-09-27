package com.raphtory.examples.random.actors

import akka.cluster.pubsub.DistributedPubSubMediator

import com.raphtory.core.actors.spout.SpoutTrait
import kamon.Kamon

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

class RandomSpout extends SpoutTrait {

  var totalCount      = 100
  var freq            = System.getenv().getOrDefault("UPDATES_FREQ", "10000").toInt    // (Updates/s) - Hz
  var increase        = System.getenv().getOrDefault("RAMP_FLAG", "false").toBoolean  // (Updates/s) - Hz
  var pool            = System.getenv().getOrDefault("ENTITY_POOL", "100000").toInt
  var msgID = 0
  override def preStart() { //set up partition to report how many messages it has processed in the last X seconds
    super.preStart()
    println(s"Prestarting ($freq Hz) Entity pool = $pool Ramp flag = $increase")
    context.system.scheduler.schedule(Duration(10, SECONDS), Duration(1, MILLISECONDS), self, "random")
    context.system.scheduler.schedule(Duration(5, MINUTES), Duration(5, MINUTES), self, "increase")
    context.system.scheduler.schedule(Duration(40, SECONDS), Duration(5, MINUTES), self, "stop")

  }

  protected def processChildMessages(rcvdMessage : Any): Unit ={
    rcvdMessage match {
      case "increase" =>
        if (increase)
          freq += 1000
      case "random" => {
        if(isSafe) {
          genRandomCommands(freq/1000)
        }
      }
      case "stop" => stop()
      case _ => println("message not recognized!")

    }

  }

  def distribution() : String = {
    val random = Random.nextFloat()
    if (random <= 0.3)      genVertexAdd()
    else if (random <= 0.7)
      genEdgeAdd()
    else if (random <= 0.8) genVertexRemoval()
    else                    genEdgeRemoval()
  }

  def genRandomCommands(number : Int) : Unit = {
    (1 to number) foreach (_ => {
      sendCommand(distribution())
    })
  }

  def genVertexAdd():String={
    s""" {"VertexAdd":{${getMessageID()}, ${genSrcID()}, ${genProperties(2)}}}"""
  }
  def genVertexAdd(src:Int):String={ //overloaded method if you want to specify src ID
    s""" {"VertexAdd":{${getMessageID()}, ${genSrcID(src)}, ${genProperties(2)}}}"""
  }

  def genVertexUpdateProperties():String={
    s""" {"VertexUpdateProperties":{${getMessageID()}, ${genSrcID(1)}}}"""
  }
  def genVertexUpdateProperties(src:Int):String={ //overloaded to mass src
    s""" {"VertexUpdateProperties":{${getMessageID()}, ${genSrcID(src)}, ${genProperties(2)}}}"""
  }

  def genVertexRemoval():String={
    s""" {"VertexRemoval":{${getMessageID()}, ${genSrcID()}}}"""
  }

  def genVertexRemoval(src:Int):String={
    s""" {"VertexRemoval":{${getMessageID()}, ${genSrcID(src)}}}"""
  }

  def genEdgeAdd():String={
    s""" {"EdgeAdd":{${getMessageID()}, ${genSrcID()}, ${genDstID()}, ${genProperties(2)}}}"""
  }
  def genEdgeAdd(src:Int,dst:Int):String={
    s""" {"EdgeAdd":{${getMessageID()}, ${genSrcID(src)}, ${genDstID(dst)}}}"""
  }

  def genEdgeUpdateProperties():String={
    s""" {"EdgeUpdateProperties":{${getMessageID()}, ${genSrcID()}, ${genDstID()}, ${genProperties(2)}}}"""
  }
  def genEdgeUpdateProperties(src:Int,dst:Int):String={
    s""" {"EdgeUpdateProperties":{${getMessageID()}, ${genSrcID(src)}, ${genDstID(dst)}, ${genProperties(2)}}}"""
  }

  def genEdgeRemoval():String={
    s""" {"EdgeRemoval":{${getMessageID()}, ${genSrcID()}, ${genDstID()}}}"""
  }
  def genEdgeRemoval(src:Int,dst:Int):String={
    s""" {"EdgeRemoval":{${getMessageID()}, ${genSrcID(src)}, ${genDstID(dst)}}}"""
  }

  def genSetSrcID():String = s""" "srcID":9 """
  def genSetDstID():String = s""" "dstID":10 """

  def genSrcID():String = s""" "srcID":${Random.nextInt(pool)} """
  def genDstID():String = s""" "dstID":${Random.nextInt(pool)} """

  def genSrcID(src:Int):String = s""" "srcID":$src """
  def genDstID(dst:Int):String = s""" "dstID":$dst """

  def getMessageID():String = {
    msgID +=1
    //s""" "messageID":${System.currentTimeMillis()} """
    s""" "messageID":${msgID} """
  }

  def genProperties(numOfProps:Int):String ={
    var properties = "\"properties\":{"
    for(i <- 1 to numOfProps){
      val propnum = i
      if(i<numOfProps) properties = properties + s""" "property$propnum":"${Random.alphanumeric.take(10).mkString}", """
      else properties = properties + s""" "property$propnum":"${Random.alphanumeric.take(10).mkString}" }"""
    }
    properties
  }


  def running() : Unit = {
    genRandomCommands(totalCount)
    //totalCount+=1000
  }

}