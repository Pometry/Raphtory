package com.raphtory.examples.bitcoin.actors

import akka.actor._
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import akka.pattern.ask
import akka.util.Timeout
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.actors.datasource.UpdaterTrait
import com.raphtory.core.model.communication.{ClusterStatusRequest, ClusterStatusResponse, SpoutGoing}
import kamon.Kamon
import spray.json._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import scalaj.http.{Http, HttpRequest}

class BitcoinSpout extends UpdaterTrait {

  var blockcount = 1
  val rpcuser = System.getenv().getOrDefault("BITCOIN_USERNAME", "user").trim
  val rpcpassword = System.getenv().getOrDefault("BITCOIN_PASSWORD", "password").trim
  val serverAddress = System.getenv().getOrDefault("BITCOIN_NODE", "bitcoinNodeURL").trim
  val id = "scala-jsonrpc"
  val baseRequest = Http(serverAddress).auth(rpcuser, rpcpassword).header("content-type", "text/plain")

  override def preStart() { //set up partition to report how many messages it has processed in the last X seconds
    super.preStart()
    context.system.scheduler.schedule(Duration(1, MINUTES), Duration(1, MILLISECONDS), self, "parseBlock")

  }

  //************* MESSAGE HANDLING BLOCK
  override def processChildMessages(message:Any): Receive = {
    case "parseBlock" => running()
    case _ => println("message not recognized!")
  }

  def running() : Unit = if(isSafe()) {
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
      BitcoinTransaction(time,blockID,transaction)
      //val time = transaction.asJsObject.fields("time")

    }
  }



}

case class BitcoinTransaction(time:JsValue,blockID:JsValue,transaction:JsValue) extends SpoutGoing