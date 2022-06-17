package com.raphtory.ethereum.graphbuilder

import com.raphtory.api.input.DoubleProperty
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.ImmutableProperty
import com.raphtory.api.input.Properties
import com.raphtory.ethereum.EthereumTransaction
import com.raphtory.api.input.Type

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

class EthereumGraphBuilder() extends GraphBuilder[String] {

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

    val edgeProperties = Properties(
            ImmutableProperty("hash", txHash),
            ImmutableProperty("blockNumber", blockNumber),
            DoubleProperty("value", value)
    )
    addVertex(
            timeStamp,
            srcID,
            Properties(
                    ImmutableProperty("address", sourceNode)
            ),
            Type("node")
    )
    addVertex(
            timeStamp,
            tarID,
            Properties(
                    ImmutableProperty("address", sourceNode)
            ),
            Type("node")
    )
    addEdge(timeStamp, srcID, tarID, edgeProperties, Type("transaction"))
  }
}
