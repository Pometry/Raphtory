package com.raphtory.examples.blockchain.bitcoin.actors

import java.io.{File, PrintWriter}

import akka.actor._
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import akka.pattern.ask
import akka.util.Timeout
import com.raphtory.core.components.Spout.SpoutTrait
import com.raphtory.core.model.communication.{ClusterStatusRequest, ClusterStatusResponse, SpoutGoing}
import com.raphtory.examples.blockchain.BitcoinTransaction
import kamon.Kamon
import spray.json._

import scala.collection.mutable
import scala.sys.process._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import scalaj.http.{Http, HttpRequest}

class BitcoinSpout extends SpoutTrait {

  var blockcount = 1
  val rpcuser = System.getenv().getOrDefault("BITCOIN_USERNAME", "").trim
  val rpcpassword = System.getenv().getOrDefault("BITCOIN_PASSWORD", "").trim
  val serverAddress = System.getenv().getOrDefault("BITCOIN_NODE", "").trim
  val id = "scala-jsonrpc"
  val baseRequest = Http(serverAddress).auth(rpcuser, rpcpassword).header("content-type", "text/plain")

  override def preStart() { //set up partition to report how many messages it has processed in the last X seconds
    super.preStart()
    context.system.scheduler.schedule(Duration(1, SECONDS),Duration(10, MILLISECONDS), self, "parseBlock")

  }

  //************* MESSAGE HANDLING BLOCK
  override def processChildMessages(message:Any): Unit = {
    message match {
      case "parseBlock" => running()
      case _ => println("message not recognized!")
    }
  }

  def running() : Unit = if(isSafe()) {
    try {
      getTransactions()
      blockcount +=1
    }
    catch {
      case e:java.net.SocketTimeoutException => "do nothing"
    }
  }


  def outputScript()={
    val pw = new PrintWriter(new File("bitcoin.sh"))
    pw.write("""curl --user $1:$2 --data-binary $3 -H 'content-type: text/plain;' $4"""
    )
    pw.close
    "chmod 777 bitcoin.sh" !
  }

  def curlRequest(command:String,params:String):String={
    //val data = """{"jsonrpc":"1.0","id":"scala-jsonrpc","method":"getblockhash","params":[2]}"""
    val data = s"""{"jsonrpc":"1.0","id":"$id","method":"$command","params":[$params]}"""
    s"bash bitcoin.sh $rpcuser $rpcpassword $data $serverAddress"!!
  }

  def request(command: String, params: String = ""): HttpRequest = baseRequest.postData(s"""{"jsonrpc": "1.0", "id":"$id", "method": "$command", "params": [$params] }""")


  def getTransactions():Unit = {
    val re = request("getblockhash",blockcount.toString).execute().body.toString.parseJson.asJsObject
    val blockID = re.fields("result")
    val blockData = request("getblock",s"$blockID,2").execute().body.toString.parseJson.asJsObject
    val result = blockData.fields("result")
    val time = result.asJsObject.fields("time")
    for(transaction <- result.asJsObject().fields("tx").asInstanceOf[JsArray].elements){

      sendCommand(BitcoinTransaction(time,blockcount,blockID,transaction))
      //val time = transaction.asJsObject.fields("time")

    }

  }



}
//def request(command: String, params: String = ""): HttpRequest = baseRequest.postData(s"""{"jsonrpc": "1.0", "id":"$id", "method": "$command", "params": [$params] }""")
