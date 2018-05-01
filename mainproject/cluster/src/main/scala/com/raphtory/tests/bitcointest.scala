package com.raphtory.tests

import scala.language.postfixOps
import spray.json._
import scalaj.http.{Http, HttpRequest }
import DefaultJsonProtocol._

object bitcointest extends App {
  val blockcount = "1"
  val rpcuser = "moe"
  val rpcpassword = "30xIeKEhn"
  val id = "scala-jsonrpc"
  val baseRequest = Http("http://moe.eecs.qmul.ac.uk:8332").auth(rpcuser, rpcpassword).header("content-type", "text/plain")

  def request(command: String, params: String = ""): HttpRequest = baseRequest.postData(s"""{"jsonrpc": "1.0", "id":"$id", "method": "$command", "params": [$params] }""")

println(getTransactions())

  def getTransactions():Unit = {
    val re = request("getblockhash",blockcount.toString).execute().body.toString.parseJson.asJsObject
    val blockID = re.fields("result")
    val blockData = request("getblock",s"$blockID,2").execute().body.toString.parseJson.asJsObject
    val result = blockData.fields("result")
    val time = result.asJsObject.fields("time")
    for(transaction <- result.asJsObject().fields("tx").asInstanceOf[JsArray].elements){
      //val time = transaction.asJsObject.fields("time")
      val txid = transaction.asJsObject.fields("txid")
      val vins = transaction.asJsObject.fields("vin")
      val vouts = transaction.asJsObject.fields("vout")
      var total:Double = 0

      for (vout <- vouts.asInstanceOf[JsArray].elements) {
        val voutOBJ = vout.asJsObject()
        var value = voutOBJ.fields("value").toString
        total+= value.toDouble
        val n = voutOBJ.fields("n").toString
        val scriptpubkey = voutOBJ.fields("scriptPubKey").asJsObject()

        var address = "nulldata"
        if(scriptpubkey.fields.contains("addresses"))
          address = scriptpubkey.fields("addresses").asInstanceOf[JsArray].elements(0).toString
        else value = "0" //TODO deal with people burning money

        sendCommand(s"""" {"VertexAdd":{ "messageID":$time , "srcID":${address.hashCode}, "properties":{"type":"address", "address":$address} }}"""") //creates vertex for the receiving wallet
        sendCommand(s"""" {"EdgeAdd":{ "messageID":$time ,  "srcID":${txid.hashCode} ,  "dstID":${address.hashCode} , "properties":{"n": $n, "value":$value}}}"""") //creates edge between the transaction and the wallet
      }

      sendCommand(s"""" {"VertexAdd":{ "messageID":$time ,  "srcID":${txid.hashCode} , "properties":{"type":"transaction", "time":$time, "id":$txid, "total": $total, "blockhash":$blockID}}}"""")

      if(vins.toString().contains("coinbase")){
        sendCommand(s"""" {"VertexAdd":{ "messageID":$time ,  "srcID":${"coingen".hashCode} ,"properties":{"type":"coingen"}}}"""") //creates the coingen node //TODO change so only added once
        sendCommand(s"""" {"EdgeAdd":{ "messageID":$time ,  "srcID":${"coingen".hashCode},  "dstID":${txid.hashCode}}}"""") //creates edge between coingen and the transaction

      }
      else{
        for(vin <- vins.asInstanceOf[JsArray].elements){
          val vinOBJ = vin.asJsObject()
          val prevVout = vinOBJ.fields("vout")
          val prevtxid = vinOBJ.fields("txid")
          //no need to create node for prevtxid as should already exist
          sendCommand(s"""" {"EdgeAdd":{ "messageID":$time ,  "srcID":${prevtxid.hashCode},  "dstID":${txid.hashCode}, "properties":{"vout":$prevVout}}}"""") //creates edge between the prev transaction and current transaction
        }
      }
    }
  }


  def sendCommand(str: String) = println(str)

  //println(curlResponse)
  //val parsedOBJ = curlResponse.toString.parseJson.asJsObject //get the json object
  //val commandKey = parsedOBJ.fields //get the command type3
  //if(commandKey.contains("error"))
  //  println(commandKey("error").compactPrint)
}