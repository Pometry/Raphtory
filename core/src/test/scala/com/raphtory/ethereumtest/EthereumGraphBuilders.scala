package com.raphtory.ethereumtest

import com.raphtory.api.input.MutableDouble
import com.raphtory.api.input.Graph
import com.raphtory.api.input.Graph.assignID
import com.raphtory.api.input.ImmutableString
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Type

object EthereumTxGraphBuilder {

  def parse(graph: Graph, tuple: String): Unit = {
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
            ImmutableString("txHash", txHash),
            ImmutableString("blockNumber", blockNumber),
            MutableDouble("value", value)
    )
    graph.addVertex(timeStamp, srcID, Properties(ImmutableString("address", sourceNode)), Type("node"))
    graph.addVertex(timeStamp, tarID, Properties(ImmutableString("address", sourceNode)), Type("node"))
    graph.addEdge(timeStamp, srcID, tarID, edgeProperties, Type("transaction"))
  }

  def TXparse(graph: Graph, tx: EthereumTransaction): Unit = {
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
            ImmutableString("txHash", txHash),
            ImmutableString("blockNumber", blockNumber),
            MutableDouble("value", value)
    )
    graph.addVertex(timeStamp, srcID, Properties(ImmutableString("address", sourceNode)), Type("node"))
    graph.addVertex(timeStamp, tarID, Properties(ImmutableString("address", sourceNode)), Type("node"))
    graph.addEdge(timeStamp, srcID, tarID, edgeProperties, Type("transaction"))
  }
}
