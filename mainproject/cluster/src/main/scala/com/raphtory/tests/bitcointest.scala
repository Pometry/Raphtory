package com.raphtory.tests

import scala.language.postfixOps
import spray.json._
import scalaj.http.{Http, HttpRequest }
import DefaultJsonProtocol._

object bitcointest extends App {
  val blockcount = "1"
  val rpcuser = ""
  val rpcpassword = ""
  val id = "scala-jsonrpc"
  val baseRequest = Http("http://moe.eecs.qmul.ac.uk:8332").auth(rpcuser, rpcpassword).header("content-type", "text/plain")

  def request(command: String, params: String = ""): HttpRequest = baseRequest.postData(s"""{"jsonrpc": "1.0", "id":"$id", "method": "$command", "params": [$params] }""")
println(getTransactions(1))

  def getTransactions(block:Int):Any = {
    val re = request("getblockhash",block.toString).execute().body.toString.parseJson.asJsObject
    val blockData = request("getblock",s"${re.fields("result")},2").execute().body.toString.parseJson.asJsObject
    val result = blockData.fields("result")

    for(transaction <- result.asJsObject().fields("tx").asInstanceOf[JsArray].elements){
      val hash = transaction.asJsObject.fields("txid")
      val vins = transaction.asJsObject.fields("vin")
      val vouts = transaction.asJsObject.fields("vout")
      if(vins.toString().contains("coinbase")){
        println(s"creating edge between coinbase and $hash")
      }
      else{
        //TODO
      }
      for(vout <- vouts.asJsObject().fields("tx").asInstanceOf[JsArray].elements){

      }
    }

  }


  //println(curlResponse)
  //val parsedOBJ = curlResponse.toString.parseJson.asJsObject //get the json object
  //val commandKey = parsedOBJ.fields //get the command type3
  //if(commandKey.contains("error"))
  //  println(commandKey("error").compactPrint)
}