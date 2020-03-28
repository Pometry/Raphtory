package com.raphtory.examples.blockchain.routers

import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication.EdgeAddWithProperties
import com.raphtory.core.model.communication.Properties
import com.raphtory.core.model.communication.StringProperty
import com.raphtory.core.model.communication.VertexAddWithProperties

import scala.util.hashing.MurmurHash3

class EthereumTransactionRouter(val routerId: Int, val initialManagerCount: Int) extends RouterWorker {

  override protected def parseTuple(value: Any): Unit = {
    val components   = value.toString.drop(1).dropRight(1).split(",")
    val creationDate = components(3).toLong * 1000 //seconds to miliseconds
    val sourceNode   = MurmurHash3.stringHash(components(0)) //hash the id to get a vertex ID
    sendGraphUpdate(
            VertexAddWithProperties(creationDate, sourceNode, Properties(StringProperty("id", components(0))))
    )                             //create the source node and add the wallet ID as a property
    if (components(1).nonEmpty) { //money being sent to an actual user
      val targetNode = MurmurHash3.stringHash(components(1)) //hash the id of the to wallet to get a vertex ID
      sendGraphUpdate(
              VertexAddWithProperties(creationDate, targetNode, Properties(StringProperty("id", components(1))))
      ) //create the destination vertex
      sendGraphUpdate(
              EdgeAddWithProperties(
                      creationDate,
                      sourceNode,
                      targetNode,
                      Properties(StringProperty("id", components(2)))
              )
      )      //create the edge between them adding the value as a property
    } else { //burnt cash
      val targetNode = MurmurHash3.stringHash("null")
      sendGraphUpdate(VertexAddWithProperties(creationDate, targetNode, Properties(StringProperty("id", "null"))))
      sendGraphUpdate(
              EdgeAddWithProperties(
                      creationDate,
                      sourceNode,
                      targetNode,
                      Properties(StringProperty("value", components(2)))
              )
      )
    }

  }
}
