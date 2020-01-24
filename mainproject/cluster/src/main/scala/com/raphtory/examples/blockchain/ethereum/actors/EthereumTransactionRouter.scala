package com.raphtory.examples.blockchain.ethereum.actors

import com.raphtory.core.components.Router.TraditionalRouter.Helpers.RouterSlave
import com.raphtory.core.model.communication.{EdgeAdd, EdgeAddWithProperties, VertexAdd, VertexAddWithProperties}

import scala.util.hashing.MurmurHash3

class EthereumTransactionRouter(val routerId:Int, val initialManagerCount:Int) extends RouterSlave{

  override protected def parseRecord(value: Any): Unit = {
    val components = value.toString.drop(1).dropRight(1).split(",")
    val creationDate = components(3).toLong*1000 //seconds to miliseconds
    val sourceNode = MurmurHash3.stringHash(components(0)) //hash the id to get a vertex ID
    toPartitionManager(VertexAddWithProperties(creationDate, sourceNode,properties = Map("id"->components(0))))//create the source node and add the wallet ID as a property
    if(components(1).nonEmpty) { //money being sent to an actual user
      val targetNode = MurmurHash3.stringHash(components(1)) //hash the id of the to wallet to get a vertex ID
      toPartitionManager(VertexAddWithProperties(creationDate, targetNode,properties = Map("id"->components(1)))) //create the destination vertex
      toPartitionManager(EdgeAddWithProperties( creationDate, sourceNode, targetNode,properties = Map("value"->components(2)))) //create the edge between them adding the value as a property
    }
    else{ //burnt cash
      val targetNode = MurmurHash3.stringHash("null")
      toPartitionManager(VertexAddWithProperties(creationDate, targetNode,properties = Map("id"->"null")))
      toPartitionManager(EdgeAddWithProperties(creationDate, sourceNode, targetNode,properties = Map("value"->components(2))))
    }

  }
}
