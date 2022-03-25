package com.raphtory.ethereum.graphbuilder

import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.graphbuilder.Properties._
import com.raphtory.ethereum.EthereumTransaction

import java.io.{BufferedReader, FileReader}
import scala.collection.mutable

class EthereumTxGraphBuilder() extends GraphBuilder[EthereumTransaction] {

  override def parseTuple(tx: EthereumTransaction): Unit = {
    //    if (line contains "block_hash") {
    //      return
    //    }
    val txHash      = tx.hash
    val blockNumber = tx.block_number.toString
    val sourceNode  = tx.from_address
    val srcID       = assignID(sourceNode)
    val targetNode  = tx.to_address
    val tarID       = assignID(targetNode)
    val timeStamp   = tx.block_timestamp
    val value       = tx.value

    val edgeProperties = Properties(
      ImmutableProperty("hash", txHash),
      ImmutableProperty("blockNumber", blockNumber),
      DoubleProperty("value", value)
    )
    addVertex(timeStamp, srcID, Properties(ImmutableProperty("address", sourceNode)), Type("node"))
    addVertex(timeStamp, tarID, Properties(ImmutableProperty("address", sourceNode)), Type("node"))
    addEdge(timeStamp, srcID, tarID, edgeProperties, Type("transaction"))
  }
}

class EthereumGraphBuilder(tagFile: String) extends GraphBuilder[String] {

  //  val br = new BufferedReader(new FileReader(tagFile))
  //  val tagMap = new mutable.HashMap[String, mutable.Set[String]]()
  //  var line : String = null
  //  while ( {line = br.readLine; line != null}) {
  //    line = br.readLine()
  //    val fileLine = line.split(",").map(_.trim)
  //    val address = fileLine(0).trim.toLowerCase
  //    val tag = fileLine(1).trim.toLowerCase
  //    if (tagMap.contains(address)) {
  //      val tagList = tagMap.getOrElse(address, mutable.Set[String]())
  //      tagList += (tag)
  //      tagMap(address) = tagList
  //    } else {
  //      tagMap += (address -> mutable.Set[String](tag))
  //    }
  //  }
  //  br.close()


  override def parseTuple(tuple: String): Unit = {
    val line        = new String(tuple)
    if (line contains "block_hash")
      return
    val fileLine    = line.replace("\"", "").split(",").map(_.trim)
    val txHash      = fileLine(0)
    val blockNumber = fileLine(2)
    val sourceNode  = fileLine(3)
    val srcID       = assignID(sourceNode)
    val targetNode  = fileLine(4)
    val tarID       = assignID(targetNode)
    val timeStamp   = fileLine(6).toLong
    val value       = fileLine(5).toDouble
    //    val sourceNodeTags = tagMap.getOrElse(sourceNode, mutable.Set[String]())
    //    val targetNodeTags = tagMap.getOrElse(targetNode, mutable.Set[String]())

    val edgeProperties = Properties(
      ImmutableProperty("hash", txHash),
      ImmutableProperty("blockNumber", blockNumber),
      DoubleProperty("value", value)
    )
    addVertex(timeStamp, srcID, Properties(
      ImmutableProperty("address", sourceNode),
      //StringProperty("tags",sourceNodeTags.toString())
    ), Type("node"))
    addVertex(timeStamp, tarID, Properties(
      ImmutableProperty("address", sourceNode),
      //StringProperty("tags",targetNodeTags.toString())
    ), Type("node"))
    addEdge(timeStamp, srcID, tarID, edgeProperties, Type("transaction"))
  }
}
