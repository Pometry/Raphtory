package com.raphtory.examples.blockchain.spouts

import com.raphtory.core.components.Spout.SpoutTrait
import com.raphtory.examples.blockchain.LitecoinTransaction
import scalaj.http.Http
import scalaj.http.HttpRequest
import spray.json._

import scala.concurrent.duration._
import scala.language.postfixOps

class LitecoinSpout extends SpoutTrait {

  var blockcount    = 1
  val rpcuser       = System.getenv().getOrDefault("LITECOIN_USERNAME", "").trim
  val rpcpassword   = System.getenv().getOrDefault("LITECOIN_PASSWORD", "").trim
  val serverAddress = System.getenv().getOrDefault("LITECOIN_NODE", "").trim
  val id            = "scala-jsonrpc"
  val baseRequest   = Http(serverAddress).auth(rpcuser, rpcpassword).header("content-type", "text/plain")

  //************* MESSAGE HANDLING BLOCK
  override def ProcessSpoutTask(message: Any): Unit = message match {
    case StartSpout   => AllocateSpoutTask(Duration(1, MILLISECONDS), "parseBlock")
    case "parseBlock" => running()
    case _            => println("message not recognized!")
  }

  def running(): Unit =
    try {
      getTransactions()
      blockcount += 1
      AllocateSpoutTask(Duration(1, NANOSECONDS), "parseBlock")
      if (blockcount % 100 == 0) println("Currently Calling for block $blockcount")
    } catch {
      case e: java.net.SocketTimeoutException     =>
      case e: spray.json.DeserializationException => AllocateSpoutTask(Duration(10, SECONDS), "parseBlock")
    }

  def getTransactions(): Unit = {
    val re        = request("getblockhash", blockcount.toString).execute().body.toString.parseJson.asJsObject
    val blockID   = re.fields("result")
    val blockData = request("getblock", s"$blockID,2").execute().body.toString.parseJson.asJsObject
    val result    = blockData.fields("result")
    val time      = result.asJsObject.fields("time")
    for (transaction <- result.asJsObject().fields("tx").asInstanceOf[JsArray].elements)
      sendTuple(LitecoinTransaction(time, blockcount, blockID, transaction))
    //val time = transaction.asJsObject.fields("time")
  }

  def request(command: String, params: String = ""): HttpRequest =
    baseRequest.postData(s"""{"jsonrpc": "1.0", "id":"$id", "method": "$command", "params": [$params] }""")

}
//def request(command: String, params: String = ""): HttpRequest = baseRequest.postData(s"""{"jsonrpc": "1.0", "id":"$id", "method": "$command", "params": [$params] }""")
