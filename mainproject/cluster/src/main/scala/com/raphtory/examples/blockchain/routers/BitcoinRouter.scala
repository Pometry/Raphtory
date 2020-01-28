package com.raphtory.examples.blockchain.routers

import com.raphtory.core.components.Router.Helpers.RouterSlave
import com.raphtory.core.model.communication.{EdgeAdd, EdgeAddWithProperties, VertexAddWithProperties}
import com.raphtory.examples.blockchain.BitcoinTransaction
import spray.json.JsArray

class BitcoinRouter(val routerId:Int, val initialManagerCount:Int) extends RouterSlave{


  def parseRecord(record: Any): Unit = {
    val value = record.asInstanceOf[BitcoinTransaction]

    val transaction = value.transaction
    val time = value.time
    val blockID = value.blockID
    val block = value.block
    val timeAsString = time.toString
    val timeAsLong = timeAsString.toLong

    val txid = transaction.asJsObject.fields("txid").toString()
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

      //println(s"Edge $timeAsLong, ${txid.hashCode}, ${address.hashCode}, $n, $value")
      //creates vertex for the receiving wallet
      toPartitionManager(VertexAddWithProperties(msgTime = timeAsLong, srcID = address.hashCode, properties = Map[String,String](("type","address"),("address",address))))
      //creates edge between the transaction and the wallet
      toPartitionManager(EdgeAddWithProperties( msgTime = timeAsLong, srcID = txid.hashCode, dstID = address.hashCode, properties = Map[String,String](("n",n),("value",value))))

    }
    toPartitionManager(VertexAddWithProperties(msgTime = timeAsLong, srcID = txid.hashCode, properties = Map[String,String](
                              ("type","transaction"),
                              ("time",timeAsString),
                              ("id",txid),
                              ("total",total.toString),
                              ("blockhash",blockID.toString),
                              ("block",block.toString))))

    if(vins.toString().contains("coinbase")){
      //creates the coingen node //TODO change so only added once
      toPartitionManager(VertexAddWithProperties(msgTime = timeAsLong, srcID = "coingen".hashCode, properties = Map[String,String](("type","coingen"))))

      //creates edge between coingen and the transaction
      toPartitionManager(EdgeAdd(msgTime = timeAsLong, srcID = "coingen".hashCode, dstID = txid.hashCode))
    }
    else{
      for(vin <- vins.asInstanceOf[JsArray].elements){
        val vinOBJ = vin.asJsObject()
        val prevVout = vinOBJ.fields("vout").toString
        val prevtxid = vinOBJ.fields("txid").toString
        //no need to create node for prevtxid as should already exist
        //creates edge between the prev transaction and current transaction
        toPartitionManager(EdgeAddWithProperties(msgTime = timeAsLong, srcID = prevtxid.hashCode, dstID = txid.hashCode, properties = Map[String,String](("vout",prevVout))))
        }
    }
  }


}


//sendCommand(s"""" {"VertexAdd":{ "messageID":$time , "srcID":${address.hashCode}, "properties":{"type":"address", "address":$address} }}"""")
//sendCommand(s"""" {"EdgeAdd":{ "messageID":$time ,  "srcID":${txid.hashCode} ,  "dstID":${address.hashCode} , "properties":{"n": $n, "value":$value}}}"""")
//sendCommand(s"""" {"VertexAdd":{ "messageID":$time ,  "srcID":${txid.hashCode} , "properties":{"type":"transaction", "time":$time, "id:$txid, "total": $total,"blockhash":$blockID}}}"""")
//sendCommand(s"""" {"VertexAdd":{ "messageID":$time ,  "srcID":${"coingen".hashCode} ,"properties":{"type":"coingen"}}}"""")
//sendCommand(s"""" {"EdgeAdd":{ "messageID":$time ,  "srcID":${"coingen".hashCode},  "dstID":${txid.hashCode}}}"""")
////sendCommand(s"""" {"EdgeAdd":{ "messageID":$time ,  "srcID":${prevtxid.hashCode},  "dstID":${txid.hashCode}, "properties":{"vout":$prevVout}}}"""")

