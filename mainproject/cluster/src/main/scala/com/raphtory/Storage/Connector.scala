package com.raphtory.Storage

trait Connector {
  def putTimeSeriesValue[T](entityType : String, entityId: Long, property: String, timestamp: Long, value: T): Boolean
  def lookup(entityType : String, entityId : Long, property : String, timestamp : Long) : Any
  def rangeQuery(entityType : String, entityId : Long, property : String, startTime : Long, endTime : Long) : Any
  def setString[T] (entityType : String, entityId : Long, property : String, timestamp : Long, value : T) : Boolean

  def lookupEntity(entityType : String, entityId : Long) : Boolean
  def saveEntity(entityType : String, entityId : Long, timestamp : Long) : Boolean
  def removeEntity(entityType : String, entityId : Long, timestamp : Long) : Boolean
}
