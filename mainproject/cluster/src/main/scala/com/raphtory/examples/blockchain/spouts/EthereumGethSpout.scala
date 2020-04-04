package com.raphtory.examples.blockchain.spouts

import java.net.InetAddress

import com.raphtory.core.components.Spout.SpoutTrait
import com.raphtory.core.utils.Utils
import com.raphtory.tests.EtherAPITest.baseRequest
import com.raphtory.tests.EtherAPITest.currentBlock
import com.raphtory.tests.EtherAPITest.request

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.duration.MILLISECONDS
import scala.concurrent.duration.NANOSECONDS
import scala.concurrent.duration.SECONDS
import scala.language.postfixOps
import scala.sys.process._
import scalaj.http.Http
import scalaj.http.HttpRequest
import spray.json._

class EthereumGethSpout extends SpoutTrait {
  var currentBlock = System.getenv().getOrDefault("SPOUT_ETHEREUM_START_BLOCK_INDEX", "4000000").trim.toInt
  var highestBlock = System.getenv().getOrDefault("SPOUT_ETHEREUM_MAXIMUM_BLOCK_INDEX", "999999999").trim.toInt
  val nodeIP       = System.getenv().getOrDefault("SPOUT_ETHEREUM_IP_ADDRESS", "127.0.0.1").trim
  val nodePort     = System.getenv().getOrDefault("SPOUT_ETHEREUM_PORT", "8545").trim
  val baseRequest  = requestBuilder()

  if(nodeIP.matches(Utils.IPRegex))
    println(s"Connecting to Ethereum RPC \n Address:$nodeIP \n Port:$nodePort")
  else
    println(s"Connecting to Ethereum RPC \n Address:${hostname2Ip(nodeIP)} \n Port:$nodePort")


  override protected def ProcessSpoutTask(message: Any): Unit = message match {
    case StartSpout  => pullNextBlock()
    case "nextBlock" => pullNextBlock()
  }

  def pullNextBlock(): Unit = {
    if (currentBlock > highestBlock)
      return
    try {

      if(debug)println(s"Trying block $currentBlock")
      val transactionCountHex =
        executeRequest("eth_getBlockTransactionCountByNumber", "\"0x" + currentBlock.toHexString + "\"")
      val transactionCount = Integer.parseInt(transactionCountHex.fields("result").toString().drop(3).dropRight(1), 16)
      for (i <- 0 until transactionCount)
        sendTuple(
                executeRequest(
                        "eth_getTransactionByBlockNumberAndIndex",
                        s""""0x${currentBlock.toHexString}","0x${i.toHexString}""""
                ).toString()
        )
      currentBlock += 1
      AllocateSpoutTask(Duration(1,NANOSECONDS),"nextBlock")
    }
    catch {
      case e:NumberFormatException => {AllocateSpoutTask(Duration(1,SECONDS),"nextBlock")}
      case e:Exception => {e.printStackTrace()};AllocateSpoutTask(Duration(1,SECONDS),"nextBlock")}
  }

  def requestBuilder() =
    if (nodeIP.matches(Utils.IPRegex))
      Http("http://" + nodeIP + ":" + nodePort).header("content-type", "application/json")
    else
      Http("http://" + hostname2Ip(nodeIP) + ":" + nodePort).header("content-type", "application/json")
  def request(command: String, params: String = ""): HttpRequest =
    baseRequest.postData(s"""{"jsonrpc": "2.0", "id":"100", "method": "$command", "params": [$params]}""")
  def executeRequest(command: String, params: String = "") =
    request(command, params).execute().body.toString.parseJson.asJsObject

  def hostname2Ip(hostname: String): String = InetAddress.getByName(hostname).getHostAddress()

}
