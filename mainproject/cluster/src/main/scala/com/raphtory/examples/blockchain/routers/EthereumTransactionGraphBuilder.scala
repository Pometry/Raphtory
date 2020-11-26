package com.raphtory.examples.blockchain.routers

import com.raphtory.core.components.Router.{GraphBuilder, RouterWorker}
import com.raphtory.core.model.communication.{EdgeAddWithProperties, GraphUpdate, Properties, StringProperty, VertexAddWithProperties}

import scala.collection.mutable.ListBuffer
import scala.collection.parallel.mutable.ParHashSet
import scala.util.hashing.MurmurHash3

class EthereumTransactionGraphBuilder extends GraphBuilder[String]{

  override def parseTuple(tuple: String):Unit = {
    val components   = tuple.drop(1).dropRight(1).split(",")
    val creationDate = components(3).toLong * 1000 //seconds to miliseconds
    val sourceNode   = MurmurHash3.stringHash(components(0)) //hash the id to get a vertex ID
    sendUpdate(
            VertexAddWithProperties(creationDate, sourceNode, Properties(StringProperty("id", components(0))))
    )                             //create the source node and add the wallet ID as a property
    if (components(1).nonEmpty) { //money being sent to an actual user
      val targetNode = MurmurHash3.stringHash(components(1)) //hash the id of the to wallet to get a vertex ID
      sendUpdate(
              VertexAddWithProperties(creationDate, targetNode, Properties(StringProperty("id", components(1))))
      ) //create the destination vertex
      sendUpdate(
              EdgeAddWithProperties(
                      creationDate,
                      sourceNode,
                      targetNode,
                      Properties(StringProperty("id", components(2)))
              )
      )      //create the edge between them adding the value as a property
    } else { //burnt cash
      val targetNode = MurmurHash3.stringHash("null")
      sendUpdate(VertexAddWithProperties(creationDate, targetNode, Properties(StringProperty("id", "null"))))
      sendUpdate(
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
