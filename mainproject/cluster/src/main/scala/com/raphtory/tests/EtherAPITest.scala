package com.raphtory.tests

import scalaj.http.{Http, HttpRequest}
import spray.json._
object EtherAPITest extends App {
  val nodeIP = System.getenv().getOrDefault("ETHEREUM_IP_ADDRESS", "http://127.0.0.1").trim
  val nodePort = System.getenv().getOrDefault("ETHEREUM_PORT", "8545").trim
  val baseRequest = Http(nodeIP+":"+nodePort).header("content-type", "application/json")
  var currentBlock = System.getenv().getOrDefault("ETHEREUM_START_BLOCK_INDEX", "4573100").trim.toInt
  var highestBlock = System.getenv().getOrDefault("ETHEREUM_MAXIMUM_BLOCK_INDEX", "999999999").trim.toInt
  def request(command: String, params: String = ""): HttpRequest = baseRequest.postData(s"""{"jsonrpc": "2.0", "id":"100", "method": "$command", "params": [$params]}""")
  val transactionCountHex = request("eth_getBlockTransactionCountByNumber","\"0x"+currentBlock.toHexString+"\"").execute().body.toString.parseJson.asJsObject
  val transactionCount = Integer.parseInt(transactionCountHex.fields("result").toString().drop(3).dropRight(1),16)
  for(i <- 0 until transactionCount) {
    println(s"At index $i")
    println(s""""0x${currentBlock.toHexString}","0x${i.toHexString}"""")
    println(request("eth_getTransactionByBlockNumberAndIndex",s""""0x${currentBlock.toHexString}","0x${i.toHexString}"""").execute().body.toString.parseJson.asJsObject)
  }
}
