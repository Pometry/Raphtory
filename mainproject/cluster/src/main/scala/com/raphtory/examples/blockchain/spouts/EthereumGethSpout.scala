package com.raphtory.examples.blockchain.spouts

import com.raphtory.core.components.Spout.SpoutTrait
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
  val nodeIP = System.getenv().getOrDefault("ETHEREUM_IP_ADDRESS", "http://127.0.0.1").trim
  val nodePort = System.getenv().getOrDefault("ETHEREUM_PORT", "8545").trim
  val baseRequest = Http(nodeIP+":"+nodePort).header("content-type", "application/json")
  override def preStart() { //set up partition to report how many messages it has processed in the last X seconds
    super.preStart()
    context.system.scheduler.scheduleOnce(Duration(1, SECONDS), self, "nextBlock")
  }

  override protected def processChildMessages(message: Any): Unit = {
    if(currentBlock>highestBlock)
      return

    message match {
      case "nextBlock" => {
        if (isSafe()) {
          pullNextBlock()
          context.system.scheduler.scheduleOnce(Duration(1, NANOSECONDS), self, "nextBlock")
        }
        else{
          context.system.scheduler.scheduleOnce(Duration(1, MILLISECONDS), self, "nextBlock")
        }
      }
      case _ => println("message not recognized!")
    }
  }

  def request(command: String, params: String = ""): HttpRequest = baseRequest.postData(s"""{"jsonrpc": "2.0", "id":"100", "method": "$command", "params": [$params]}""")
  def executeRequest(command: String, params: String = "") = request(command,params).execute().body.toString.parseJson.asJsObject
  def pullNextBlock() = {
    val transactionCountHex = executeRequest("eth_getBlockTransactionCountByNumber","\"0x"+currentBlock.toHexString+"\"")
    val transactionCount = Integer.parseInt(transactionCountHex.fields("result").toString().drop(3).dropRight(1),16)
    for(i <- 0 until transactionCount) {
       sendCommand(executeRequest("eth_getTransactionByBlockNumberAndIndex",s""""0x${currentBlock.toHexString}","0x${i.toHexString}"""").toString())
    }
    currentBlock+=1
  }

}
