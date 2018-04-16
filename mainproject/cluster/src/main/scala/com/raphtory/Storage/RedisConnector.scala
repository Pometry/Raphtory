package com.raphtory.Storage

import java.util.concurrent.ConcurrentSkipListSet

import com.raphtory.GraphEntities.Entity
import com.redis.RedisClient

object RedisConnector extends Connector {
  private val redis : RedisClient = new RedisClient()("localhost", 6379)
  private val historyKey : String = "history"

  private def getPropertyKey(entityType : String, entityId : Long, property : String, timestamp : Long) : String = {
    s"$entityType:$entityId:$property:$timestamp"
  }

  private def getEntityKey(entityType : String) : String = {
    entityType
  }

  private def getEntityType[T <: Entity](entity : T) : String = {
    entity.getClass.getSimpleName
  }

  override def putTimeSeriesValue[T](entityType : String, entityId: Long, property: String, timestamp: Long, value: T): Boolean = {
    redis.append(getPropertyKey(entityType, entityId, property, timestamp), value) match {
      case Some(ret) => true
      case None      => false
    }
  }

  /**
    * Lookup operation on Redis, it should return the right dataType (TODO as generic) or just return a string
    * @param entityType
    * @param entityId
    * @param property
    * @param timestamp
    * @return
    */
  override def lookup(entityType : String, entityId : Long, property : String, timestamp : Long) : Any = {
    // TODO
  }


  /**
    * RangeQuery operation on Redis. Read lookup TODO notes
    * @param entityType
    * @param entityId
    * @param property
    * @param startTime
    * @param endTime
    * @return
    */
  override def rangeQuery(entityType : String, entityId : Long, property : String, startTime : Long, endTime : Long) : Any = {
    // TODO
  }

  def setString[T] (entityType : String, entityId : Long, property : String, timestamp : Long, value : T) : Boolean =
    redis.set(getPropertyKey(entityType, entityId, property, timestamp), value)

  override def saveEntity(entityType : String, entityId : Long, timestamp : Long) : Boolean = {
    //redis.hm
    val entityKey = getEntityKey(entityType)
    lookupEntity(entityType, entityId) match {
      case true => putTimeSeriesValue(entityKey, entityId, historyKey, timestamp, true)
      case false => redis.sadd(entityKey, entityId); true // TODO
    }
  }

  override def removeEntity(entityType : String, entityId : Long, timestamp : Long) : Boolean = {
    putTimeSeriesValue(entityType, entityId, historyKey, timestamp, false)
  }

  override def lookupEntity(entityType: String, entityId: Long) : Boolean = {
    redis.sismember(entityType, entityId)
  }


  def getEntities(key : String) : Set[Long] = {
    redis.smembers(key) match {
      case None => Set[Long]()
      case Some(set) => {
        set.map(opEl => opEl match {
          case Some(s) => s.toLong
        })
      }
    }
  }

  def addAssociatedEdge(vertexId : Long, edgeIndex : Long) : Boolean = {
    redis
  }
}
