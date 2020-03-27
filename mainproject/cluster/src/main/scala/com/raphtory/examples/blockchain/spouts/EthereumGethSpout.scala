package com.raphtory.examples.blockchain.spouts

import java.net.InetAddress

import com.raphtory.core.components.Spout.SpoutTrait
import com.raphtory.core.utils.Utils
import com.raphtory.tests.EtherAPITest.{baseRequest, currentBlock, request}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, MILLISECONDS, NANOSECONDS, SECONDS}
import scala.language.postfixOps
import scala.sys.process._
import scalaj.http.{Http, HttpRequest}
import spray.json._

class EthereumGethSpout extends SpoutTrait {
  var currentBlock = System.getenv().getOrDefault("ETHEREUM_START_BLOCK_INDEX", "5000000").trim.toInt
  var highestBlock = System.getenv().getOrDefault("ETHEREUM_MAXIMUM_BLOCK_INDEX", "999999999").trim.toInt
  val nodeIP = System.getenv().getOrDefault("ETHEREUM_IP_ADDRESS", "127.0.0.1").trim
  val nodePort = System.getenv().getOrDefault("ETHEREUM_PORT", "8545").trim
  val baseRequest = requestBuilder()

  if(nodeIP.matches(Utils.IPRegex))
    println(s"Connecting to Ethereum RPC \n Address:$nodeIP \n Port:$nodePort")
  else
    println(s"Connecting to Ethereum RPC \n Address:${hostname2Ip(nodeIP)} \n Port:$nodePort")


  override protected def ProcessSpoutTask(message: Any): Unit = message match {
      case StartSpout => pullNextBlock()
      case "nextBlock" => pullNextBlock()
    }

  def pullNextBlock():Unit = {
    if(currentBlock>highestBlock)
      return
    try {
      println(s"Trying block $currentBlock")
      val transactionCountHex = executeRequest("eth_getBlockTransactionCountByNumber", "\"0x" + currentBlock.toHexString + "\"")
      val transactionCount = Integer.parseInt(transactionCountHex.fields("result").toString().drop(3).dropRight(1), 16)
      for (i <- 0 until transactionCount)
        sendTuple(executeRequest("eth_getTransactionByBlockNumberAndIndex", s""""0x${currentBlock.toHexString}","0x${i.toHexString}"""").toString())
      currentBlock += 1
      AllocateSpoutTask(Duration(1,MILLISECONDS),"nextBlock")
    }
    catch {
      case e:NumberFormatException => {currentBlock +=1;AllocateSpoutTask(Duration(1,MILLISECONDS),"nextBlock")}
      case e:Exception => {currentBlock +=1;e.printStackTrace()};AllocateSpoutTask(Duration(1,MILLISECONDS),"nextBlock")}
  }

  def requestBuilder() = {
    if(nodeIP.matches(Utils.IPRegex))
      Http("http://"+nodeIP+":"+nodePort).header("content-type", "application/json")
    else
      Http("http://"+hostname2Ip(nodeIP)+":"+nodePort).header("content-type", "application/json")
  }
  def request(command: String, params: String = ""): HttpRequest = baseRequest.postData(s"""{"jsonrpc": "2.0", "id":"100", "method": "$command", "params": [$params]}""")
  def executeRequest(command: String, params: String = "") = request(command,params).execute().body.toString.parseJson.asJsObject


  def hostname2Ip(hostname: String): String = InetAddress.getByName(hostname).getHostAddress()

}
