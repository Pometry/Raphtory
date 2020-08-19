package com.raphtory.examples.blockchain.spouts

import java.io.File
import java.io.PrintWriter

import com.raphtory.core.components.Spout.SpoutTrait
import com.raphtory.examples.blockchain.BitcoinTransaction
import scalaj.http.Http
import scalaj.http.HttpRequest
import spray.json._

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.sys.process._

class BitcoinSpout extends SpoutTrait {

  var blockcount    = 1
  val rpcuser       = System.getenv().getOrDefault("BITCOIN_USERNAME", "").trim
  val rpcpassword   = System.getenv().getOrDefault("BITCOIN_PASSWORD", "").trim
  val serverAddress = System.getenv().getOrDefault("BITCOIN_NODE", "").trim
  val id            = "scala-jsonrpc"
  val baseRequest   = Http(serverAddress).auth(rpcuser, rpcpassword).header("content-type", "text/plain")

  //************* MESSAGE HANDLING BLOCK
  override def ProcessSpoutTask(message: Any): Unit = message match {
    case StartSpout => AllocateSpoutTask(Duration(1, MILLISECONDS), "parseBlock")
    case "parseBlock" =>
      try {
        getTransactions()
        blockcount += 1
        AllocateSpoutTask(Duration(1, MILLISECONDS), "parseBlock")
      } catch {
        case e: java.net.SocketTimeoutException => AllocateSpoutTask(Duration(1, MILLISECONDS), "parseBlock")
      }
    case _ => println("message not recognized!")
  }

  def outputScript() = {
    val pw = new PrintWriter(new File("bitcoin.sh"))
    pw.write("""curl --user $1:$2 --data-binary $3 -H 'content-type: text/plain;' $4""")
    pw.close
    "chmod 777 bitcoin.sh" !
  }

  def curlRequest(command: String, params: String): String = {
    //val data = """{"jsonrpc":"1.0","id":"scala-jsonrpc","method":"getblockhash","params":[2]}"""
    val data = s"""{"jsonrpc":"1.0","id":"$id","method":"$command","params":[$params]}"""
    s"bash bitcoin.sh $rpcuser $rpcpassword $data $serverAddress" !!
  }

  def request(command: String, params: String = ""): HttpRequest =
    baseRequest.postData(s"""{"jsonrpc": "1.0", "id":"$id", "method": "$command", "params": [$params] }""")

  def getTransactions(): Unit = {
    val re        = request("getblockhash", blockcount.toString).execute().body.toString.parseJson.asJsObject
    val blockID   = re.fields("result")
    val blockData = request("getblock", s"$blockID,2").execute().body.toString.parseJson.asJsObject
    val result    = blockData.fields("result")
    val time      = result.asJsObject.fields("time")
    for (transaction <- result.asJsObject().fields("tx").asInstanceOf[JsArray].elements)
      sendTuple(BitcoinTransaction(time, blockcount, blockID, transaction))
    //val time = transaction.asJsObject.fields("time")

  }

}
//def request(command: String, params: String = ""): HttpRequest = baseRequest.postData(s"""{"jsonrpc": "1.0", "id":"$id", "method": "$command", "params": [$params] }""")
