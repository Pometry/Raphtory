package com.raphtory.examples.bitcoin.actors

import akka.actor._
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import akka.pattern.ask
import akka.util.Timeout
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.model.communication.{ClusterStatusRequest, ClusterStatusResponse}
import kamon.Kamon
import spray.json._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import scalaj.http.{Http, HttpRequest}

class BitcoinSpout extends RaphtoryActor with Timers {

  val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)


  var currentMessage  = 0
  var previousMessage = 0
  var safe            = false
  var counter         = 0

  var blockcount = 1
  val rpcuser = System.getenv().getOrDefault("BITCOIN_USERNAME", "user").trim
  val rpcpassword = System.getenv().getOrDefault("BITCOIN_PASSWORD", "password").trim
  val serverAddress = System.getenv().getOrDefault("BITCOIN_NODE", "bitcoinNodeURL").trim
  val id = "scala-jsonrpc"
  val baseRequest = Http(serverAddress).auth(rpcuser, rpcpassword).header("content-type", "text/plain")

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

  def running() : Unit = if(safe) {
    getTransactions()
    blockcount +=1
  }

  def request(command: String, params: String = ""): HttpRequest = baseRequest.postData(s"""{"jsonrpc": "1.0", "id":"$id", "method": "$command", "params": [$params] }""")

  def getTransactions():Unit = {
    val re = request("getblockhash",blockcount.toString).execute().body.toString.parseJson.asJsObject
    val blockID = re.fields("result")
    val blockData = request("getblock",s"$blockID,2").execute().body.toString.parseJson.asJsObject
    val result = blockData.fields("result")
    val time = result.asJsObject.fields("time")
    for(transaction <- result.asJsObject().fields("tx").asInstanceOf[JsArray].elements){
      //val time = transaction.asJsObject.fields("time")
      val txid = transaction.asJsObject.fields("txid")
      val vins = transaction.asJsObject.fields("vin")
      val vouts = transaction.asJsObject.fields("vout")
      var total:Double = 0

      for (vout <- vouts.asInstanceOf[JsArray].elements) {
        val voutOBJ = vout.asJsObject()
        var value = voutOBJ.fields("value").toString
        total+= value.toDouble
        val n = voutOBJ.fields("n").toString
        val scriptpubkey = voutOBJ.fields("scriptPubKey").asJsObject()

        var address = "nulldata"
        if(scriptpubkey.fields.contains("addresses"))
          address = scriptpubkey.fields("addresses").asInstanceOf[JsArray].elements(0).toString
        else value = "0" //TODO deal with people burning money

        sendCommand(s"""" {"VertexAdd":{ "messageID":$time , "srcID":${address.hashCode}, "properties":{"type":"address", "address":$address} }}"""") //creates vertex for the receiving wallet
        sendCommand(s"""" {"EdgeAdd":{ "messageID":$time ,  "srcID":${txid.hashCode} ,  "dstID":${address.hashCode} , "properties":{"n": $n, "value":$value}}}"""") //creates edge between the transaction and the wallet
      }

      sendCommand(s"""" {"VertexAdd":{ "messageID":$time ,  "srcID":${txid.hashCode} , "properties":{"type":"transaction", "time":$time, "id:$txid, "total": $total,"blockhash":$blockID}}}"""")

      if(vins.toString().contains("coinbase")){
        sendCommand(s"""" {"VertexAdd":{ "messageID":$time ,  "srcID":${"coingen".hashCode} ,"properties":{"type":"coingen"}}}"""") //creates the coingen node //TODO change so only added once
        sendCommand(s"""" {"EdgeAdd":{ "messageID":$time ,  "srcID":${"coingen".hashCode},  "dstID":${txid.hashCode}}}"""") //creates edge between coingen and the transaction

      }
      else{
        for(vin <- vins.asInstanceOf[JsArray].elements){
          val vinOBJ = vin.asJsObject()
          val prevVout = vinOBJ.fields("vout")
          val prevtxid = vinOBJ.fields("txid")
          //no need to create node for prevtxid as should already exist
          sendCommand(s"""" {"EdgeAdd":{ "messageID":$time ,  "srcID":${prevtxid.hashCode},  "dstID":${txid.hashCode}, "properties":{"vout":$prevVout}}}"""") //creates edge between the prev transaction and current transaction
        }
      }
    }
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
