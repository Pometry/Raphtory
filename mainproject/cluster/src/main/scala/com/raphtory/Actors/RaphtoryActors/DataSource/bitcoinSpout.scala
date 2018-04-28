package com.raphtory.Actors.RaphtoryActors.DataSource

import java.io.FileWriter
import spray.json._

import akka.actor._
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import akka.pattern.ask
import akka.util.Timeout
import com.raphtory.Actors.RaphtoryActors.RaphtoryActor
import com.raphtory.caseclass.{ClusterStatusRequest, ClusterStatusResponse}
import kamon.Kamon

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps
import scala.util.Random
import scala.sys.process._

class bitcoinSpout extends RaphtoryActor with Timers {

  val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)


  var currentMessage  = 0
  var previousMessage = 0
  var safe            = false
  var counter         = 0
  var blockcount = 1

  val username = System.getenv().getOrDefault("BITCOIN_USERNAME", "user").trim
  val password = System.getenv().getOrDefault("BITCOIN_PASSWORD", "password").trim

  override def preStart() { //set up partition to report how many messages it has processed in the last X seconds
    context.system.scheduler.schedule(Duration(1, MINUTES), Duration(1, MILLISECONDS), self, "parseBlock")
    context.system.scheduler.schedule(Duration(7, SECONDS), Duration(1, SECONDS), self,"benchmark")
    context.system.scheduler.schedule(Duration(7, SECONDS), Duration(1, SECONDS), self,"stateCheck")
    context.system.scheduler.schedule(Duration(0, SECONDS), Duration(10, SECONDS), self,"increaseFreq") // TODO delete

  }

  //************* MESSAGE HANDLING BLOCK
  override def receive: Receive = {
    case "stateCheck" => checkUp()
    case "benchmark" => benchmark()
    case "parseBlock" => running()
    case _ => println("message not recognized!")
  }

  def running() : Unit = if(safe) parseBlock()

  def parseBlock() = {
    getblockhash(blockcount)

    "ping moe.eecs.amul.ac.uk" !

  }

  def getblockhash(blockcount: Int):String = {
    //example {"result":"0000000000002917ed80650c6174aac8dfc46f5fe36480aaef682ff6cd83c3ca","error":null,"id":"curltext"}
    val curlResponse =  s"""curl --user $username:$password --data-binary '{"jsonrpc":"1.0","id":"curltext","method":"getblockhash","params":[$blockcount]}' -H 'content-type:text/plain;' http://moe.eecs.qmul.ac.uk:8332/""" !
    val parsedOBJ = curlResponse.toString.parseJson.asJsObject //get the json object
    val commandKey = parsedOBJ.fields //get the command type
    if(commandKey.contains("error"))
      println(commandKey("error").compactPrint)
    return ""
  }

  def sendCommand(command: String) ={
    counter += 1
    currentMessage+=1
    mediator ! DistributedPubSubMediator.Send("/user/router", command, false)
    Kamon.counter("raphtory.updateGen.commandsSent").increment()
    kGauge.refine("actor" -> "Updater", "name" -> "updatesSentGauge").set(counter)
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





}
