package com.raphtory.ethereumtest

import com.raphtory.components.graphbuilder.GraphBuilder
import com.raphtory.components.graphbuilder.Properties._

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
            ImmutableProperty("txHash", txHash),
            ImmutableProperty("blockNumber", blockNumber),
            DoubleProperty("value", value)
    )
    addVertex(timeStamp, srcID, Properties(ImmutableProperty("address", sourceNode)), Type("node"))
    addVertex(timeStamp, tarID, Properties(ImmutableProperty("address", sourceNode)), Type("node"))
    addEdge(timeStamp, srcID, tarID, edgeProperties, Type("transaction"))
  }
}

class EthereumGraphBuilder() extends GraphBuilder[String] {

  override def parseTuple(tuple: String): Unit = {
    val line        = new String(tuple)
    if (line contains "block_hash")
      return
    val fileLine    = line.replace("\"", "").split(",").map(_.trim)
    val txHash      = fileLine(0)
    val blockNumber = fileLine(3)
    val sourceNode  = fileLine(5)
    val srcID       = assignID(sourceNode)
    val targetNode  = fileLine(6)
    val tarID       = assignID(targetNode)
    val timeStamp   = fileLine(11).toLong
    val value       = fileLine(7).toDouble

    val edgeProperties = Properties(
            ImmutableProperty("txHash", txHash),
            ImmutableProperty("blockNumber", blockNumber),
            DoubleProperty("value", value)
    )
    addVertex(timeStamp, srcID, Properties(ImmutableProperty("address", sourceNode)), Type("node"))
    addVertex(timeStamp, tarID, Properties(ImmutableProperty("address", sourceNode)), Type("node"))
    addEdge(timeStamp, srcID, tarID, edgeProperties, Type("transaction"))
  }
}
