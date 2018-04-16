package com.raphtory.Controller

import com.raphtory.Storage.{Connector, RedisConnector}

object GraphRepoProxy {
  private val connectors : Array[Connector] = Array(RedisConnector) // TODO add Raphtory Cache Implementation

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

  def addEntity(entityType : String, entityId : Long, timestamp : Long) : Boolean = {
    connectors.foreach(conn => {
      conn.saveEntity(entityType, entityId, timestamp)
    })
    true // TODO what?
  }

  def delEntity(entityType : String, entityId : Long, timestamp : Long) : Boolean = {
    connectors.foreach(conn => {
      conn.removeEntity(entityType, entityId, timestamp)
    })
    true // TODO what?
  }

  // TODO
  // getHistory
  // getProperties
  // updateProperty
  // getProperty
  // getEntity

}
