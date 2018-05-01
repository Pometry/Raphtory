package com.raphtory.Actors.RaphtoryActors

import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.caseclass._
import com.raphtory.utils.Utils
import kamon.Kamon
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, SECONDS}
import Utils.getManager
import com.raphtory.Actors.RaphtoryActors.Router.RouterTrait
import monix.eval.Task
import monix.execution.{ExecutionModel, Scheduler}

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

class RaphtoryRouter(routerId:Int,initialManagerCount:Int) extends RaphtoryActor {
  var managerCount : Int = initialManagerCount  // TODO check for initial behavior (does the watchdog stop the router?)
  val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)
  //************* MESSAGE HANDLING BLOCK
  println(akka.serialization.Serialization.serializedActorPath(self))
  var count = 0

  implicit val s : Scheduler = Scheduler(ExecutionModel.BatchedExecution(1024))

  override def preStart() {
    context.system.scheduler.schedule(Duration(7, SECONDS),
      Duration(1, SECONDS),self,"tick")
    context.system.scheduler.schedule(Duration(8, SECONDS),
      Duration(10, SECONDS), self, "keep_alive")
  }

  def keepAlive() = mediator ! DistributedPubSubMediator.Send("/user/WatchDog", RouterUp(routerId), false)

  override def receive: Receive = {
    case "tick" => {
      kGauge.refine("actor" -> "Router", "name" -> "count").set(count)
      count = 0
    }
    case "keep_alive" => keepAlive()

    case command:String =>  Task.eval(parseJSON(command)).fork.runAsync

   // case PartitionsCount(newValue) => { // TODO redundant in Router and LAM (https://stackoverflow.com/questions/37596888/scala-akka-implement-abstract-class-with-subtype-parameter)
    case UpdatedCounter(newValue) => {
      if (managerCount < newValue)
        managerCount = newValue
      println(s"Maybe a new PartitionManager has arrived: ${newValue}")
    }

    case e => println(s"message not recognized! ${e.getClass}")
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

  def vertexAdd(command:JsObject):Unit = {
   // println("Inside add")
    val msgTime = command.fields("messageID").toString().toLong
    val srcId = command.fields("srcID").toString().toInt                 //extract the srcID
    if(command.fields.contains("properties")) {                          //if there are properties within the command
      var properties = Map[String,String]()                              //create a vertex map
      command.fields("properties").asJsObject.fields.foreach( pair => {  //add all of the pairs to the map
        properties = properties updated (pair._1, pair._2.toString())
      })
      //send the srcID and properties to the graph manager
      mediator ! DistributedPubSubMediator.Send(getManager(srcId,managerCount),VertexAddWithProperties(msgTime,srcId,properties),false)
      // println(s"sending vertex add $srcId to Manager 1")
    }
    else {
      mediator ! DistributedPubSubMediator.Send(getManager(srcId,managerCount),VertexAdd(msgTime,srcId),false)
      // println(s"sending vertex add $srcId to Manager 1")
    } // if there are not any properties, just send the srcID
  }

  def vertexUpdateProperties(command:JsObject):Unit={
    val msgTime = command.fields("messageID").toString().toLong
    val srcId = command.fields("srcID").toString().toInt //extract the srcID
    var properties = Map[String,String]() //create a vertex map
    command.fields("properties").asJsObject.fields.foreach( pair => {properties = properties updated (pair._1,pair._2.toString())})
    mediator ! DistributedPubSubMediator.Send(getManager(srcId,managerCount),VertexUpdateProperties(msgTime,srcId,properties),false) //send the srcID and properties to the graph parition
  }

  def vertexRemoval(command:JsObject):Unit={
    val msgTime = command.fields("messageID").toString().toLong
    val srcId = command.fields("srcID").toString().toInt //extract the srcID
    mediator ! DistributedPubSubMediator.Send(getManager(srcId,managerCount),VertexRemoval(msgTime,srcId),false)
  }

  def edgeAdd(command:JsObject):Unit = {
    val msgTime = command.fields("messageID").toString().toLong
    val srcId = command.fields("srcID").toString().toInt //extract the srcID
    val dstId = command.fields("dstID").toString().toInt //extract the dstID
    if(command.fields.contains("properties")){ //if there are properties within the command
    var properties = Map[String,String]() //create a vertex map
      command.fields("properties").asJsObject.fields.foreach( pair => { //add all of the pairs to the map
        properties = properties updated (pair._1,pair._2.toString())
      })
      mediator ! DistributedPubSubMediator.Send(getManager(srcId,managerCount),EdgeAddWithProperties(msgTime,srcId,dstId,properties),false) //send the srcID, dstID and properties to the graph manager
    }
    else mediator ! DistributedPubSubMediator.Send(getManager(srcId,managerCount),EdgeAdd(msgTime,srcId,dstId),false)
  }

  def edgeUpdateProperties(command:JsObject):Unit={
    val msgTime = command.fields("messageID").toString().toLong
    val srcId = command.fields("srcID").toString().toInt //extract the srcID
    val dstId = command.fields("dstID").toString().toInt //extract the dstID
    var properties = Map[String,String]() //create a vertex map
    command.fields("properties").asJsObject.fields.foreach( pair => {properties = properties updated (pair._1,pair._2.toString())})
    mediator ! DistributedPubSubMediator.Send(getManager(srcId,managerCount),EdgeUpdateProperties(msgTime,srcId,dstId,properties),false) //send the srcID, dstID and properties to the graph manager
  }

  def edgeRemoval(command:JsObject):Unit={
    val msgTime = command.fields("messageID").toString().toLong
    val srcId = command.fields("srcID").toString().toInt //extract the srcID
    val dstId = command.fields("dstID").toString().toInt //extract the dstID
    mediator ! DistributedPubSubMediator.Send(getManager(srcId,managerCount),EdgeRemoval(msgTime,srcId,dstId),false) //send the srcID, dstID to graph manager
  }

}
