package com.raphtory.core.actors.router

import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.actors.router.WindowingRouter
import com.raphtory.core.model.communication._
import com.raphtory.core.utils.Utils.getManager
import kamon.Kamon
import monix.eval.Task
import monix.execution.{ExecutionModel, Scheduler}
import spray.json._

import scala.concurrent.duration.{Duration, SECONDS}

final class RaphtoryWindowingRouter(override val routerId:Int, override val initialManagerCount:Int) extends WindowingRouter {
  var count = 0


  override def preStart() {
    super.preStart()
  }

  def keepAlive() = mediator ! DistributedPubSubMediator.Send("/user/WatchDog", RouterUp(routerId), false)

  override def parseJSON(command:String):Unit={
    count += 1
    kCounter.refine("actor" -> "Router", "name" -> "count").increment()
    Kamon.gauge("raphtory.router.countGauge").set(count)
    //println(s"received command: \n $command")
    val parsedOBJ = command.parseJson.asJsObject //get the json object
    val commandKey = parsedOBJ.fields //get the command type
    if(commandKey.contains("VertexAdd")) vertexAdd(parsedOBJ.getFields("VertexAdd").head.asJsObject)
    else if(commandKey.contains("VertexUpdateProperties")) vertexUpdateProperties(parsedOBJ.getFields("VertexUpdateProperties").head.asJsObject)
    //else if(commandKey.contains("VertexRemoval")) vertexRemoval(parsedOBJ.getFields("VertexRemoval").head.asJsObject)
    else if(commandKey.contains("EdgeAdd")) edgeAdd(parsedOBJ.getFields("EdgeAdd").head.asJsObject) //if addVertex, parse to handling function
    else if(commandKey.contains("EdgeUpdateProperties")) edgeUpdateProperties(parsedOBJ.getFields("EdgeUpdateProperties").head.asJsObject)
    //else if(commandKey.contains("EdgeRemoval")) edgeRemoval(parsedOBJ.getFields("EdgeRemoval").head.asJsObject)
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
      mediator ! DistributedPubSubMediator.Send(getManager(srcId,getManagerCount),VertexAddWithProperties(routerId,msgTime,srcId,properties),false)
      // println(s"sending vertex add $srcId to Manager 1")
    }
    else {
      mediator ! DistributedPubSubMediator.Send(getManager(srcId,getManagerCount),VertexAdd(routerId,msgTime,srcId),false)
      // println(s"sending vertex add $srcId to Manager 1")
    } // if there are not any properties, just send the srcID

    //Add into our router map
    super.addVertex(srcId)

  }

  def vertexUpdateProperties(command:JsObject):Unit={
    val msgTime = command.fields("messageID").toString().toLong
    val srcId = command.fields("srcID").toString().toInt //extract the srcID
    var properties = Map[String,String]() //create a vertex map
    command.fields("properties").asJsObject.fields.foreach( pair => {properties = properties updated (pair._1,pair._2.toString())})
    mediator ! DistributedPubSubMediator.Send(getManager(srcId,getManagerCount),VertexUpdateProperties(routerId,msgTime,srcId,properties),false) //send the srcID and properties to the graph parition

    //Add into our router map
    super.addVertex(srcId)
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
      mediator ! DistributedPubSubMediator.Send(getManager(srcId,getManagerCount),EdgeAddWithProperties(routerId,msgTime,srcId,dstId,properties),false) //send the srcID, dstID and properties to the graph manager
    }
    else mediator ! DistributedPubSubMediator.Send(getManager(srcId,getManagerCount),EdgeAdd(routerId,msgTime,srcId,dstId),false)

    //Add into our router map
    super.addEdge(srcId,dstId)
  }

  def edgeUpdateProperties(command:JsObject):Unit={
    val msgTime = command.fields("messageID").toString().toLong
    val srcId = command.fields("srcID").toString().toInt //extract the srcID
    val dstId = command.fields("dstID").toString().toInt //extract the dstID
    var properties = Map[String,String]() //create a vertex map
    command.fields("properties").asJsObject.fields.foreach( pair => {properties = properties updated (pair._1,pair._2.toString())})
    mediator ! DistributedPubSubMediator.Send(getManager(srcId,getManagerCount),EdgeUpdateProperties(routerId,msgTime,srcId,dstId,properties),false) //send the srcID, dstID and properties to the graph manager

    //Add into our router map
    super.addEdge(srcId,dstId)
  }

}
