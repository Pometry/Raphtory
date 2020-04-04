package com.raphtory.examples.blockchain.routers

import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication.EdgeAddWithProperties
import com.raphtory.core.model.communication.ImmutableProperty
import com.raphtory.core.model.communication.Properties
import com.raphtory.core.model.communication.StringProperty
import com.raphtory.core.model.communication.VertexAddWithProperties
import spray.json._

import scala.util.hashing.MurmurHash3
class EthereumGethRouter(override val routerId: Int, val initialManagerCount: Int) extends RouterWorker {
  def hexToInt(hex: String) = Integer.parseInt(hex.drop(3).dropRight(1), 16)
  //
  override protected def parseTuple(value: Any): Unit = {
    val transaction = value.toString.parseJson.asJsObject.fields("result").asJsObject
    val blockNumber = hexToInt(transaction.fields("blockNumber").toString())

    val from = transaction.fields("from").toString().replaceAll("\"", "").toLowerCase
    val to   = transaction.fields("to").toString().replaceAll("\"", "").toLowerCase
    val sent = transaction.fields("value").toString().replaceAll("\"", "")

    val sourceNode      = assignID(from) //hash the id to get a vertex ID
    val destinationNode = assignID(to)   //hash the id to get a vertex ID

    sendGraphUpdate(
            VertexAddWithProperties(blockNumber, sourceNode, properties = Properties(ImmutableProperty("id", from)))
    )
    sendGraphUpdate(
            VertexAddWithProperties(blockNumber, destinationNode, properties = Properties(ImmutableProperty("id", to)))
    )
    sendGraphUpdate(
            EdgeAddWithProperties(
                    blockNumber,
                    sourceNode,
                    destinationNode,
                    properties = Properties(StringProperty("value", sent))
            )
    )

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
