package com.raphtory.dev.ethereum.graphbuilder

import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.model.graph.{ImmutableProperty, Properties, StringProperty, LongProperty, DoubleProperty, Type}
import org.apache.avro.generic.GenericRecord

import java.io.{BufferedReader, FileReader}
import scala.collection.mutable

class EthereumGraphBuilderBuffer extends GraphBuilder[GenericRecord]{

  override def parseTuple(ethTx: GenericRecord) = {
    val tagFile = System.getenv().getOrDefault("TAG_FILE", "/app/tags.csv").trim
    //val tagFile = "/Users/Haaroony/OneDrive/OneDrive - University College London/PhD/Projects/scala/raphtory-ethereum2/resources/etherscan_tags.csv"
    // Load tags
    val br = new BufferedReader(new FileReader(tagFile))
    val tagMap = new mutable.HashMap[String, mutable.Set[String]]()
    var line : String = null
    while ( {line = br.readLine; line != null}) {
      line = br.readLine()
      val fileLine = line.split(",").map(_.trim)
      val address = fileLine(0).trim
      val tag = fileLine(1).trim
      if (tagMap.contains(address)) {
        var tagList = tagMap.getOrElse(address, mutable.Set[String]() )
        tagList += (tag)
        tagMap(address) = tagList
      } else {
        tagMap += (address -> mutable.Set[String](tag))
      }
    }
    br.close()

    val hash = ethTx.get("hash").toString
    if (hash!="hash"){
      val blockNumber = ethTx.get("block_number").toString.toLong
      val txindex = ethTx.get("transaction_index").toString.toLong
      val sourceNode = if(ethTx.get("from_address") == null) "none" else ethTx.get("from_address").toString
      val srcID      = assignID(sourceNode)
      val targetNode = if(ethTx.get("to_address") == null) "none" else ethTx.get("to_address").toString
      val tarID = assignID(targetNode)
      val sourceNodeTags = tagMap.getOrElse(sourceNode, mutable.Set[String]())
      val targetNodeTags = tagMap.getOrElse(targetNode, mutable.Set[String]())
      val value      = ethTx.get("value").toString.toFloat / 1e18
      val timeStamp  = ethTx.get("block_timestamp").toString.toLong
      addVertex(timeStamp, srcID, Properties(
        ImmutableProperty("name",sourceNode),
        StringProperty("tags",sourceNodeTags.toString())
      ),Type("Address"))
      addVertex(timeStamp, tarID, Properties(
        ImmutableProperty("name",targetNode),
        StringProperty("tags",targetNodeTags.toString())
      ),Type("Address"))
      addEdge(timeStamp,srcID,tarID, Properties(
        StringProperty("hash", hash),
        LongProperty("blockNumber", blockNumber),
        DoubleProperty("value", value)
      )
      )
    }

  }
}
