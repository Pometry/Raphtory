package com.raphtory.examples.blockchain.spouts

import com.raphtory.core.components.Spout.SpoutTrait
import com.raphtory.examples.blockchain.LitecoinTransaction
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import scalaj.http.Http
import scalaj.http.HttpRequest
import spray.json._

import scala.concurrent.duration._
import scala.language.postfixOps

class DashcoinSpout extends SpoutTrait {

  var blockcount    = 1
  val rpcuser       = System.getenv().getOrDefault("DASHCOIN_USERNAME", "").trim
  val rpcpassword   = System.getenv().getOrDefault("DASHCOIN_PASSWORD", "").trim
  val serverAddress = System.getenv().getOrDefault("DASHCOIN_NODE", "").trim
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
      for (i <- 1 to 10) {
        getTransactions()
        blockcount += 1
        if (blockcount % 1000 == 0) println(s"Parsed block $blockcount at ${DateTime.now(DateTimeZone.UTC).getMillis}")
      }
      AllocateSpoutTask(Duration(1, NANOSECONDS), "parseBlock")

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
