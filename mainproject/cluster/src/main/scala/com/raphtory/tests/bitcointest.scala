package com.raphtory.tests

import scala.language.postfixOps
import spray.json._

import scalaj.http.{Http, HttpRequest}
import DefaultJsonProtocol._
import java.io._
import scala.sys.process._


import com.raphtory.tests.bitcointest.sendCommand

import scala.io.Source

object bitcointest extends App {
  var blockcount = "1"
  val rpcuser = ""
  val rpcpassword = ""
  val id = "scala-jsonrpc"
  val address = "http://eecs.qmul.ac.uk:8332"
  val baseRequest = Http("http://eecs.qmul.ac.uk:8332").auth(rpcuser, rpcpassword).header("content-type", "text/plain")

  def request(command: String, params: String = ""): HttpRequest = baseRequest.postData(s"""{"jsonrpc": "1.0", "id":"$id", "method": "$command", "params": [$params] }""")

  def outputScript()={
    val pw = new PrintWriter(new File("bitcoin.sh"))
    pw.write("echo $3\n")
    pw.write("""curl --user $1:$2 --data-binary $3 -H 'content-type: text/plain;' $4"""
    )
    pw.close
  }

  def curlRequest(command:String,params:String)={
    //val data = """{"jsonrpc":"1.0","id":"scala-jsonrpc","method":"getblockhash","params":[2]}"""
    val data = s"""{"jsonrpc":"1.0","id":"$id","method":"$command","params":[$params]}"""
    s"bash bitcoin.sh $rpcuser $rpcpassword $data $address"!
  }

  outputScript()
  println(curlRequest("getblockhash","2"))
  //val blockID = re.fields("result")
  //val blockData = request("getblock",s"$blockID,2").execute().body.toString.parseJson.asJsObject

//  getTransactions()
 // readTransactions()
  //for(i <- 1 to 20000){
  //  blockcount = i.toString
  // saveTransactions()
  //}

  def saveTransactions()={
    val re = request("getblockhash",blockcount.toString).execute().body.toString.parseJson.asJsObject
    val blockID = re.fields("result")
    val blockData = request("getblock",s"$blockID,2").execute().body.toString.parseJson.asJsObject
    val result = blockData.fields("result").prettyPrint
    println(s"got block $blockcount")
    val file = new File(s"blocks/$blockcount.txt")
    val pw = new PrintWriter(file)
    pw.write(result)
    pw.close

  }

  def readTransactions()={
    val bufferedSource = Source.fromFile(s"blocks/$blockcount.txt")
    var block =""
    for (line <- bufferedSource.getLines) {
      block +=line
    }
    bufferedSource.close


    val result = block.parseJson.asJsObject
    val time = result.fields("time")
    val blockID = result.fields("hash")

    for(transaction <- result.asJsObject().fields("tx").asInstanceOf[JsArray].elements) {
      //val time = transaction.asJsObject.fields("time")
      val txid = transaction.asJsObject.fields("txid")
      val vins = transaction.asJsObject.fields("vin")
      val vouts = transaction.asJsObject.fields("vout")
      var total: Double = 0

      for (vout <- vouts.asInstanceOf[JsArray].elements) {
        val voutOBJ = vout.asJsObject()
        var value = voutOBJ.fields("value").toString
        total += value.toDouble
        val n = voutOBJ.fields("n").toString
        val scriptpubkey = voutOBJ.fields("scriptPubKey").asJsObject()

        var address = "nulldata"
        if (scriptpubkey.fields.contains("addresses"))
          address = scriptpubkey.fields("addresses").asInstanceOf[JsArray].elements(0).toString
        else value = "0" //TODO deal with people burning money

        sendCommand(s"""" {"VertexAdd":{ "messageID":$time , "srcID":${address.hashCode}, "properties":{"type":"address", "address":$address} }}"""") //creates vertex for the receiving wallet
        sendCommand(s"""" {"EdgeAdd":{ "messageID":$time ,  "srcID":${txid.hashCode} ,  "dstID":${address.hashCode} , "properties":{"n": $n, "value":$value}}}"""") //creates edge between the transaction and the wallet
      }

      sendCommand(s"""" {"VertexAdd":{ "messageID":$time ,  "srcID":${txid.hashCode} , "properties":{"type":"transaction", "time":$time, "id":$txid, "total": $total, "blockhash":$blockID}}}"""")

      if (vins.toString().contains("coinbase")) {
        sendCommand(s"""" {"VertexAdd":{ "messageID":$time ,  "srcID":${"coingen".hashCode} ,"properties":{"type":"coingen"}}}"""") //creates the coingen node //TODO change so only added once
        sendCommand(s"""" {"EdgeAdd":{ "messageID":$time ,  "srcID":${"coingen".hashCode},  "dstID":${txid.hashCode}}}"""") //creates edge between coingen and the transaction

      }
      else {
        for (vin <- vins.asInstanceOf[JsArray].elements) {
          val vinOBJ = vin.asJsObject()
          val prevVout = vinOBJ.fields("vout")
          val prevtxid = vinOBJ.fields("txid")
          //no need to create node for prevtxid as should already exist
          sendCommand(s"""" {"EdgeAdd":{ "messageID":$time ,  "srcID":${prevtxid.hashCode},  "dstID":${txid.hashCode}, "properties":{"vout":$prevVout}}}"""") //creates edge between the prev transaction and current transaction
        }
      }
    }

  }

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

}