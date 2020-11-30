package com.raphtory.examples.blockchain.graphbuilders

import com.raphtory.core.actors.Router.GraphBuilder
import com.raphtory.core.model.communication._

import scala.util.Random
class FirehoseKafkaGraphBuilder extends GraphBuilder[String] {
  var DELETEPERCENT = System.getenv().getOrDefault("ETHER_DELETE_PERCENT", "0").trim.toDouble/100
  var DELETESEED = System.getenv().getOrDefault("ETHER_DELETE_SEED", "123").trim.toInt
  val random = new Random(DELETESEED)
  def hexToInt(hex: String) = Integer.parseInt(hex.drop(2), 16)

  override def parseTuple(tuple: String): Unit = {
    //if(value.toString.contains("0xa09871aeadf4994ca12f5c0b6056bbd1d343c029")) println(value.toString)
    val transaction = tuple.split(",")
    if(transaction(1).equals("block_number")) return
    val blockNumber = transaction(2).toInt

    val from = transaction(4).replaceAll("\"", "").toLowerCase
    val to   = transaction(5).replaceAll("\"", "").toLowerCase
    val sent = (BigDecimal(transaction(6).replaceAll("\"", ""))/BigDecimal("1000000000000000000")).toDouble
    val sourceNode      = assignID(from) //hash the id to get a vertex ID
    val destinationNode = assignID(to)   //hash the id to get a vertex ID
    //if(from.contains("0xa09871aeadf4994ca12f5c0b6056bbd1d343c029".toLowerCase())) println(from)
    //if(to.contains("0xa09871aeadf4994ca12f5c0b6056bbd1d343c029".toLowerCase())) println(to)
    sendUpdate(VertexAddWithProperties(blockNumber, sourceNode, properties = Properties(ImmutableProperty("id", from))))
//    if(random.nextDouble()<=DELETEPERCENT)
//      sendGraphUpdate(VertexDelete(blockNumber+1,sourceNode))

    sendUpdate(VertexAddWithProperties(blockNumber, destinationNode, properties = Properties(ImmutableProperty("id", to))))
//    if(random.nextDouble()<=DELETEPERCENT)
//      sendGraphUpdate(VertexDelete(blockNumber+1,destinationNode))

    sendUpdate(EdgeAddWithProperties(blockNumber, sourceNode, destinationNode,properties = Properties(DoubleProperty("value", sent))))
//    if(random.nextDouble()<=DELETEPERCENT)
//     sendGraphUpdate(EdgeDelete(blockNumber+1,sourceNode,destinationNode))
  }
}

//{
//  "jsonrpc": "2.0",
//  "id": 100,
//  "result": {
//    "blockHash": "0x4e3a3754410177e6937ef1f84bba68ea139e8d1a2258c5f85db9f1cd715a1bdd",
//    "blockNumber": "0xb443",
//    "from": "0xa1e4380a3b1f749673e270229993ee55f35663b4",
//    "gas": "0x5208",
//    "gasPrice": "0x2d79883d2000",
//    "hash": "0x5c504ed432cb51138bcf09aa5e8a410dd4a1e204ef84bfed1be16dfba1b22060",
//    "input": "0x",
//    "nonce": "0x0",
//    "to": "0x5df9b87991262f6ba471f09758cde1c0fc1de734",
//    "transactionIndex": "0x0",
//    "value": "0x7a69",
//    "v": "0x1c",
//    "r": "0x88ff6cf0fefd94db46111149ae4bfc179e9b94721fffd821d38d16464b3f71d0",
//    "s": "0x45e0aff800961cfce805daef7016b9b675c137a6a41a548f7b60a3484c06a33a"
//  }
//}
