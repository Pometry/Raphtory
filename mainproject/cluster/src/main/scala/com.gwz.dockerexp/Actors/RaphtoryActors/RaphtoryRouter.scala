package com.gwz.dockerexp.Actors.RaphtoryActors

import java.util.Calendar

import akka.actor.Actor
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.gwz.dockerexp.caseclass._
import kamon.Kamon
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, SECONDS}

/**
  * The Graph Manager is the top level actor in this system (under the stream)
  * which tracks all the graph partitions - passing commands processed by the 'command processor' actors
  * to the correct partition
  */

/**
  * The Command Processor takes string message from Kafka and translates them into
  * the correct case Class which can then be passed to the graph manager
  * which will then pass it to the graph partition dealing with the associated vertex
  */

class RaphtoryRouter(managerCount:Int) extends RaphtoryActor{
  val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)
  //************* MESSAGE HANDLING BLOCK

  var count = 0

  override def preStart() {
    context.system.scheduler.schedule(Duration(7, SECONDS),Duration(2, SECONDS),self,"tick")
  }

  override def receive: Receive = {
    case "tick" => {
      // TODO put router ID here
      kGauge.refine("actor" -> "Router", "name" -> "count").set(count)
      count = 0
    }
    case command:String => try{parseJSON(command)}catch {case e: Exception => println(e)}
    case _ => println("message not recognized!")
  }
  def parseJSON(command:String):Unit={
    count += 1
    kCounter.refine("actor" -> "Router", "name" -> "count").increment()
    Kamon.gauge("raphtory.router.countGauge").set(count)
    //println(s"received command: \n $command")
    val parsedOBJ = command.parseJson.asJsObject //get the json object
    val commandKey = parsedOBJ.fields //get the command type
    if(commandKey.contains("VertexAdd")) vertexAdd(parsedOBJ.getFields("VertexAdd").head.asJsObject)
    else if(commandKey.contains("VertexUpdateProperties")) vertexUpdateProperties(parsedOBJ.getFields("VertexUpdateProperties").head.asJsObject)
    else if(commandKey.contains("VertexRemoval")) vertexRemoval(parsedOBJ.getFields("VertexRemoval").head.asJsObject)
    else if(commandKey.contains("EdgeAdd")) edgeAdd(parsedOBJ.getFields("EdgeAdd").head.asJsObject) //if addVertex, parse to handling function
    else if(commandKey.contains("EdgeUpdateProperties")) edgeUpdateProperties(parsedOBJ.getFields("EdgeUpdateProperties").head.asJsObject)
    else if(commandKey.contains("EdgeRemoval")) edgeRemoval(parsedOBJ.getFields("EdgeRemoval").head.asJsObject)
  }
//************ END MESSAGE HANDLING BLOCK

  def vertexAdd(command:JsObject):Unit = {
   // println("Inside add")
    val msgId = command.fields("messageID").toString().toInt
    val srcId = command.fields("srcID").toString().toInt                 //extract the srcID
    if(command.fields.contains("properties")) {                          //if there are properties within the command
      var properties = Map[String,String]()                              //create a vertex map
      command.fields("properties").asJsObject.fields.foreach( pair => {  //add all of the pairs to the map
        properties = properties updated (pair._1, pair._2.toString())
      })
      //send the srcID and properties to the graph manager
      mediator ! DistributedPubSubMediator.Send(getManager(srcId),VertexAddWithProperties(msgId,srcId,properties),false)
      // println(s"sending vertex add $srcId to Manager 1")
    }
    else {
      mediator ! DistributedPubSubMediator.Send(getManager(srcId),VertexAdd(msgId,srcId),false)
      // println(s"sending vertex add $srcId to Manager 1")
    } // if there are not any properties, just send the srcID
  }

  def vertexUpdateProperties(command:JsObject):Unit={
    val msgId = command.fields("messageID").toString().toInt
    val srcId = command.fields("srcID").toString().toInt //extract the srcID
    var properties = Map[String,String]() //create a vertex map
    command.fields("properties").asJsObject.fields.foreach( pair => {properties = properties updated (pair._1,pair._2.toString())})
    mediator ! DistributedPubSubMediator.Send(getManager(srcId),VertexUpdateProperties(msgId,srcId,properties),false) //send the srcID and properties to the graph parition
  }

  def vertexRemoval(command:JsObject):Unit={
    val msgId = command.fields("messageID").toString().toInt
    val srcId = command.fields("srcID").toString().toInt //extract the srcID
    mediator ! DistributedPubSubMediator.Send(getManager(srcId),VertexRemoval(msgId,srcId),false)
  }

  def edgeAdd(command:JsObject):Unit = {
    val msgId = command.fields("messageID").toString().toInt
    val srcId = command.fields("srcID").toString().toInt //extract the srcID
    val dstId = command.fields("dstID").toString().toInt //extract the dstID
    if(command.fields.contains("properties")){ //if there are properties within the command
    var properties = Map[String,String]() //create a vertex map
      command.fields("properties").asJsObject.fields.foreach( pair => { //add all of the pairs to the map
        properties = properties updated (pair._1,pair._2.toString())
      })
      mediator ! DistributedPubSubMediator.Send(getManager(srcId),EdgeAddWithProperties(msgId,srcId,dstId,properties),false) //send the srcID, dstID and properties to the graph manager
    }
    else mediator ! DistributedPubSubMediator.Send(getManager(srcId),EdgeAdd(msgId,srcId,dstId),false)
  }

  def edgeUpdateProperties(command:JsObject):Unit={
    val msgId = command.fields("messageID").toString().toInt
    val srcId = command.fields("srcID").toString().toInt //extract the srcID
    val dstId = command.fields("dstID").toString().toInt //extract the dstID
    var properties = Map[String,String]() //create a vertex map
    command.fields("properties").asJsObject.fields.foreach( pair => {properties = properties updated (pair._1,pair._2.toString())})
    mediator ! DistributedPubSubMediator.Send(getManager(srcId),EdgeUpdateProperties(msgId,srcId,dstId,properties),false) //send the srcID, dstID and properties to the graph manager
  }

  def edgeRemoval(command:JsObject):Unit={
    val msgId = command.fields("messageID").toString().toInt
    val srcId = command.fields("srcID").toString().toInt //extract the srcID
    val dstId = command.fields("dstID").toString().toInt //extract the dstID
    mediator ! DistributedPubSubMediator.Send(getManager(srcId),EdgeRemoval(msgId,srcId,dstId),false) //send the srcID, dstID to graph manager
  }

  def getManager(srcId:Int):String = s"/user/Manager_${srcId % managerCount}" //simple srcID hash at the moment

}
