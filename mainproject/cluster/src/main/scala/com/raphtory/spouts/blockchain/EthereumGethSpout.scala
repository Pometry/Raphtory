package com.raphtory.spouts.blockchain

import java.net.InetAddress
import java.util.NoSuchElementException

import com.raphtory.core.components.Spout.Spout
import com.raphtory.core.utils.Utils
import scalaj.http.{Http, HttpRequest}
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.mutable
import scala.language.postfixOps



class EthereumGethSpout extends Spout[String] {

  var currentBlock = System.getenv().getOrDefault("SPOUT_ETHEREUM_START_BLOCK_INDEX", "9014194").trim.toInt
  var highestBlock = System.getenv().getOrDefault("SPOUT_ETHEREUM_MAXIMUM_BLOCK_INDEX", "10026447").trim.toInt
  val nodeIP       = System.getenv().getOrDefault("SPOUT_ETHEREUM_IP_ADDRESS", "127.0.0.1").trim
  val nodePort     = System.getenv().getOrDefault("SPOUT_ETHEREUM_PORT", "8545").trim
//   val nodePort     = System.getenv().getOrDefault("SPOUT_ETHEREUM_PORT", "30303").trim

  print(currentBlock)

  val queue = mutable.Queue[Option[String]]()

  val baseRequest  = requestBuilder()

  override def setupDataSource(): Unit = {}
  override def closeDataSource(): Unit = {}
  override def generateData(): Option[String] = {
    if(queue.isEmpty)
      pullNextBlock()
    queue.dequeue()
  }





  implicit val EthFormat = jsonFormat14(EthResult)
  implicit val EthTransactionFormat = jsonFormat3(EthTransaction)
  if (nodeIP.matches(Utils.IPRegex))
    println(s"Connecting to Ethereum RPC \n Address:$nodeIP \n Port:$nodePort")
  else
    println(s"Connecting to Ethereum RPC \n Address:${hostname2Ip(nodeIP)} \n Port:$nodePort")


  def pullNextBlock(): Unit = {
    if (currentBlock > highestBlock)
      return
    try {
      val transactionCountHex = executeRequest("eth_getBlockTransactionCountByNumber", "\"0x" + currentBlock.toHexString + "\"");
      val transactionCount = Integer.parseInt(transactionCountHex.fields("result").toString().drop(3).dropRight(1), 16)
      if(transactionCount>0){
        var transactions = "["
        for (i <- 0 until transactionCount)
          transactions = transactions + batchRequestBuilder("eth_getTransactionByBlockNumberAndIndex",s""""0x${currentBlock.toHexString}","0x${i.toHexString}"""")+","
        val trasnactionBlock = executeBatchRequest(transactions.dropRight(1)+"]")
        val transList = trasnactionBlock.parseJson.convertTo[List[EthTransaction]]
        transList.foreach(t => { //try needed to ignore contracts //todo include them
          try{queue +=(Some(s"${t.result.blockNumber.get},${t.result.from.get},${t.result.to.get},${t.result.value.get}"))}
          catch {case e:NoSuchElementException =>}

        })

      }
      currentBlock += 1
    } catch {
      case e: NumberFormatException =>
      case e: Exception             => e.printStackTrace();
    }
  }


  def batchRequestBuilder(command:String,params:String):String = s"""{"jsonrpc": "2.0", "id":"100", "method": "$command", "params": [$params]}"""
  def executeBatchRequest(data: String) = requestBatch(data).execute().body.toString
  def requestBatch(data: String): HttpRequest = baseRequest.postData(data)
  def requestBuilder() =
    if (nodeIP.matches(Utils.IPRegex))
      Http("http://" + nodeIP + ":" + nodePort).header("content-type", "application/json")
    else
      Http("http://" + hostname2Ip(nodeIP) + ":" + nodePort).header("content-type", "application/json")
  def request(command: String, params: String = ""): HttpRequest =
    baseRequest.postData(s"""{"jsonrpc": "2.0", "id":"100", "method": "$command", "params": [$params]}""")

  def executeRequest(command: String, params: String = "") = {
    request(command, params).execute().body.toString.parseJson.asJsObject
  }

  def hostname2Ip(hostname: String): String = InetAddress.getByName(hostname).getHostAddress()


}

case class EthResult(blockHash:Option[String],blockNumber:Option[String],from:Option[String],gas:Option[String],gasPrice:Option[String],hash:Option[String],input:Option[String],nonce:Option[String],r:Option[String],s:Option[String],to:Option[String],transactionIndex:Option[String],v:Option[String],value:Option[String])
case class EthTransaction(id:Option[String],jsonrpc:Option[String],result:EthResult)

