package com.raphtory.Controller

import com.raphtory.Storage.{Connector, RedisConnector}

object GraphRepoProxy {
  private val connectors : Array[Connector] = Array(RedisConnector) // TODO add Raphtory Cache Implementation

  private var edgesSet : Set[Long] = Set[Long]()
  private var verticesSet : Set[Long] = Set[Long]()

  def apply(): Unit = {
  }

  // TODO iterative apply
  //def iterativeApply(f : Connector => Unit)

  def entityExists(entityType : String, id : Long) : Boolean = {
    connectors.foreach(conn => {
      if (conn.lookupEntity(entityType, id))
        return true
    })
    false
  }

  def addEdge(id : Long) = {
    edgesSet += id
  }

  def addVertex(id : Long) = {
    verticesSet += id
  }

  def getEdges() : Set[Long] = {
    edgesSet
  }

  def getVertices() : Set[Long] = {
    verticesSet
  }

  // TODO
  // getHistory
  // getProperties
  // getProperty
  // getEntity

}
