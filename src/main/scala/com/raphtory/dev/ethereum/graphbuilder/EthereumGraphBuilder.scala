package com.raphtory.dev.ethereum.graphbuilder

import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.model.graph._
import com.raphtory.dev.ethereum.EthereumTransaction

import java.io.{BufferedReader, FileReader}
import scala.collection.mutable

class EthereumGraphBuilder extends GraphBuilder[EthereumTransaction]{
  val tagFile = System.getenv().getOrDefault("TAG_FILE", "/app/tags.csv").trim
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
      val tagList = tagMap.getOrElse(address, mutable.Set[String]())
      tagList += (tag)
      tagMap(address) = tagList
    } else {
      tagMap += (address -> mutable.Set[String](tag))
    }
  }
  br.close()

  override def parseTuple(ethTx: EthereumTransaction) = {
    val hash = ethTx.hash
    if (hash!="hash"){
      val blockNumber = ethTx.block_number.toString.toLong
      val txindex = ethTx.transaction_index.toString.toLong
      val sourceNode = if(ethTx.from_address == null) "none" else ethTx.from_address.toString
      val srcID      = assignID(sourceNode)
      val targetNode = if(ethTx.to_address == null) "none" else ethTx.to_address.toString
      val tarID = assignID(targetNode)
      val sourceNodeTags = tagMap.getOrElse(sourceNode, mutable.Set[String]())
      val targetNodeTags = tagMap.getOrElse(targetNode, mutable.Set[String]())
      val value      = ethTx.value.toString.toFloat / 1e18
      val timeStamp  = ethTx.block_timestamp.toString.toLong
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
