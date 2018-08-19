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
  var freq            = System.getenv().getOrDefault("UPDATES_FREQ", "1000").toInt    // (Updates/s) - Hz
  var increase        = System.getenv().getOrDefault("RAMP_FLAG", "false").toBoolean  // (Updates/s) - Hz

  override def preStart() { //set up partition to report how many messages it has processed in the last X seconds
    super.preStart()
    println(s"Prestarting ($freq Hz)")
    context.system.scheduler.schedule(Duration(10, SECONDS), Duration(1, MILLISECONDS), self, "random")
    context.system.scheduler.schedule(Duration(5, MINUTES), Duration(5, MINUTES), self, "increase")
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
      case _ => println("message not recognized!")

    }
  }

  def distribution() : String = {
    val random = Random.nextFloat()
    if (random <= 0.3)      genVertexAdd()
    else if (random <= 0.7) genEdgeAdd()
    else if (random <= 0.8) genVertexRemoval()
    else                    genEdgeRemoval()
  }

  def genRandomCommands(number : Int) : Unit = {
    (1 to number) foreach (_ => {
      sendCommand(distribution())
    })
  }

  def genVertexAdd():String={
    s""" {"VertexAdd":{${getMessageID()}, ${genSrcID()}, ${genProperties(2,true)}}}"""
  }
  def genVertexAdd(src:Int):String={ //overloaded method if you want to specify src ID
    s""" {"VertexAdd":{${getMessageID()}, ${genSrcID(src)}, ${genProperties(2,true)}}}"""
  }

  def genVertexUpdateProperties():String={
    s""" {"VertexUpdateProperties":{${getMessageID()}, ${genSrcID(1)}}}"""
  }
  def genVertexUpdateProperties(src:Int):String={ //overloaded to mass src
    s""" {"VertexUpdateProperties":{${getMessageID()}, ${genSrcID(src)}, ${genProperties(2,true)}}}"""
  }

  def genVertexRemoval():String={
    s""" {"VertexRemoval":{${getMessageID()}, ${genSrcID()}}}"""
  }

  def genVertexRemoval(src:Int):String={
    s""" {"VertexRemoval":{${getMessageID()}, ${genSrcID(src)}}}"""
  }

  def genEdgeAdd():String={
    s""" {"EdgeAdd":{${getMessageID()}, ${genSrcID()}, ${genDstID()}}}"""
  }
  def genEdgeAdd(src:Int,dst:Int):String={
    s""" {"EdgeAdd":{${getMessageID()}, ${genSrcID(src)}, ${genDstID(dst)}}}"""
  }

  def genEdgeUpdateProperties():String={
    s""" {"EdgeUpdateProperties":{${getMessageID()}, ${genSrcID()}, ${genDstID()}}}"""
  }
  def genEdgeUpdateProperties(src:Int,dst:Int):String={
    s""" {"EdgeUpdateProperties":{${getMessageID()}, ${genSrcID(src)}, ${genDstID(dst)}, ${genProperties(2,true)}}}"""
  }

  def genEdgeRemoval():String={
    s""" {"EdgeRemoval":{${getMessageID()}, ${genSrcID()}, ${genDstID()}}}"""
  }
  def genEdgeRemoval(src:Int,dst:Int):String={
    s""" {"EdgeRemoval":{${getMessageID()}, ${genSrcID(src)}, ${genDstID(dst)}}}"""
  }

  def genSetSrcID():String = s""" "srcID":9 """
  def genSetDstID():String = s""" "dstID":10 """
  def genSrcID():String = s""" "srcID":${Random.nextInt(100)} """
  def genDstID():String = s""" "dstID":${Random.nextInt(100)} """
  def genSrcID(src:Int):String = s""" "srcID":$src """
  def genDstID(dst:Int):String = s""" "dstID":$dst """

  def getMessageID():String = s""" "messageID":${System.currentTimeMillis()} """

  def genProperties(numOfProps:Int,randomProps:Boolean):String ={
    var properties = "\"properties\":{"
    for(i <- 1 to numOfProps){
      val propnum = {if(randomProps) Random.nextInt(2) else i}
      if(i<numOfProps) properties = properties + s""" "property$propnum":${Random.nextInt()}, """
      else properties = properties + s""" "property$propnum":${Random.nextInt()} }"""
    }
    properties
  }

  def running() : Unit = {
    genRandomCommands(totalCount)
    //totalCount+=1000
  }

}
