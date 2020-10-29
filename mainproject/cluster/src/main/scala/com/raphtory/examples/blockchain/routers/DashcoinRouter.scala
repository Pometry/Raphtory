package com.raphtory.examples.blockchain.routers

import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication.{EdgeAdd, EdgeAddWithProperties, GraphUpdate, Properties, StringProperty, StringSpoutGoing, VertexAdd, VertexAddWithProperties}
import com.raphtory.spouts.blockchain.BitcoinTransaction
import spray.json.JsArray

import scala.collection.mutable.ListBuffer
import scala.util.hashing.MurmurHash3

class DashcoinRouter(override val routerId: Int,override val workerID:Int, override val initialManagerCount: Int)
  extends RouterWorker[BitcoinTransaction](routerId,workerID, initialManagerCount) {

  override protected def parseTuple(tuple: BitcoinTransaction): List[GraphUpdate] = {
    val transaction  = tuple.transaction
    val time         = tuple.time
    val blockID      = tuple.blockID
    val block        = tuple.block
    val timeAsString = time.toString
    val timeAsLong   = (timeAsString.toLong) * 1000

    val txid          = transaction.asJsObject.fields("txid").toString()
    val vins          = transaction.asJsObject.fields("vin")
    val vouts         = transaction.asJsObject.fields("vout")
    val locktime      = transaction.asJsObject.fields("locktime")
    val version       = transaction.asJsObject.fields("version")
    var total: Double = 0
    val commands = new ListBuffer[GraphUpdate]()
    for (vout <- vouts.asInstanceOf[JsArray].elements) {
      val voutOBJ = vout.asJsObject()
      var value   = voutOBJ.fields("value").toString
      total += value.toDouble
      val n            = voutOBJ.fields("n").toString
      val scriptpubkey = voutOBJ.fields("scriptPubKey").asJsObject()

      var address    = "nulldata"
      val outputType = scriptpubkey.fields("type").toString

      if (scriptpubkey.fields.contains("addresses"))
        address = scriptpubkey.fields("addresses").asInstanceOf[JsArray].elements(0).toString
      else value = "0" //TODO deal with people burning money

      //creates vertex for the receiving wallet
      commands+=(
              VertexAddWithProperties(
                      msgTime = timeAsLong,
                      srcID = MurmurHash3.stringHash(address),
                      properties = Properties(
                              //("type","address"),
                              StringProperty("address", address),
                              StringProperty("outputType", outputType)
                      )
              )
      )
      //creates edge between the transaction and the wallet
      commands+=(
              EdgeAddWithProperties(
                      msgTime = timeAsLong,
                      srcID = MurmurHash3.stringHash(txid),
                      dstID = MurmurHash3.stringHash(address),
                      properties = Properties(StringProperty("n", n), StringProperty("value", value))
              )
      )

    }
    commands+=(
            VertexAddWithProperties(
                    msgTime = timeAsLong,
                    srcID = MurmurHash3.stringHash(txid),
                    properties = Properties(
                            //("type","transaction"),
                            StringProperty("id", txid),
                            StringProperty("total", total.toString),
                            //("lockTime",locktime.toString),
                            //("version",version.toString),
                            // ("blockhash",blockID.toString),
                            StringProperty("block", block.toString)
                    )
            )
    )

    if (vins.toString().contains("coinbase")) {
      //creates the coingen node
      //toPartitionManager(VertexAddWithProperties(msgTime = timeAsLong, srcID = MurmurHash3.stringHash("coingen"), properties = Map[String,String](("type","coingen"))))
      commands+=(VertexAdd(msgTime = timeAsLong, srcID = MurmurHash3.stringHash("coingen")))

      //creates edge between coingen and the transaction
      commands+=(
              EdgeAdd(
                      msgTime = timeAsLong,
                      srcID = MurmurHash3.stringHash("coingen"),
                      dstID = MurmurHash3.stringHash(txid)
              )
      )
    } else
      for (vin <- vins.asInstanceOf[JsArray].elements) {
        val vinOBJ   = vin.asJsObject()
        val prevVout = vinOBJ.fields("vout").toString
        val prevtxid = vinOBJ.fields("txid").toString
        val sequence = vinOBJ.fields("sequence").toString
        //no need to create node for prevtxid as should already exist
        //creates edge between the prev transaction and current transaction
        commands+=(
                EdgeAddWithProperties(
                        msgTime = timeAsLong,
                        srcID = MurmurHash3.stringHash(prevtxid),
                        dstID = MurmurHash3.stringHash(txid),
                        properties = Properties(StringProperty("vout", prevVout), StringProperty("sequence", sequence))
                )
        )

      }
    commands.toList
  }

}
