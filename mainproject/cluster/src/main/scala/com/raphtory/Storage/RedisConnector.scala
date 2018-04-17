package com.raphtory.Storage

import com.raphtory.GraphEntities.{Entity, Property, Vertex, Edge}
import com.raphtory.utils.{KeyEnum, SubKeyEnum}
import com.redis.RedisClient
import com.raphtory.utils.exceptions._

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

object RedisConnector extends Connector {
  private val redis : RedisClient = new RedisClient()("localhost", 6379)

  /**
    * Add the entity and set its creation time (if it doesn't exist)
    * @param entityType
    * @param entityId
    * @param creationTime
    */
  override def addEntity(entityType : KeyEnum.Value, entityId : Long, creationTime : Long) = {
    redis.sadd(entityType, entityId)
    redis.sadd(s"$entityType:$entityId:${SubKeyEnum.creationTime}", creationTime)
  }

  /**
    * add a new change on the state of the Entity (history)
    * @param entityType
    * @param entityId
    * @param timestamp
    * @param value
    */
  override def addState(entityType : KeyEnum.Value, entityId : Long, timestamp : Long, value : Boolean) = {
    addProperty(entityType, entityId, SubKeyEnum.history.toString, timestamp, value.toString)
  }

  /**
    * add a new value for a given property at a given time
    * @param entityType
    * @param entityId
    * @param key
    * @param timestamp
    * @param value
    */
  override def addProperty(entityType : KeyEnum.Value, entityId : Long, key : String, timestamp : Long, value : String) = {
    redis.set(s"${KeyEnum.vertices}:$entityId:$key:$timestamp", value)
  }

  /**
    * add one associated vertex to the set of associatedEdges for the given Vertex
    * @param vertexId
    * @param edgeId
    */
  override def addAssociatedVertex(vertexId : Long, edgeId : Long) = {
    redis.sadd(s"${KeyEnum.vertices}:$vertexId:${KeyEnum.edges}", edgeId)
  }

  /**
    * check for the existence of the given $entityId in the $entityType set
    * @param entityType
    * @param entityId
    * @return
    */
  override def lookupEntity(entityType: String, entityId: Long) : Boolean = {
    redis.sismember(entityType, entityId)
  }

  /**
    * Get the list of the entities ids (vertex or edges)
    * @param entityType
    * @return
    */
  override def getEntities(entityType : KeyEnum.Value) : Set[Long] = {
    redis.smembers(entityType) match {
      case None => Set[Long]()
      case Some(set) => {
        set.map(opEl => opEl match {
          case Some(s) => s.toLong
        })
      }
    }
  }

  /**
    * Load from Redis the Entity datas
    * @param entityType
    * @param entityId
    * @return
    */
  override def getEntity(entityType: KeyEnum.Value, entityId: Long) : Option[_ <: Entity] = {
    entityType match {
      case KeyEnum.vertices => Some(getVertex(entityId.toInt))
      case KeyEnum.edges    => Some(getEdge(entityId))
      case _                => throw new IllegalArgumentException
    }
  }

  /**
    * Load all of the Redis-stored graph
    * @param entityType
    * @return
    */
  override def getEntitiesObjs(entityType: KeyEnum.Value) = {
    val entities = TrieMap[Long, Entity]()
    getEntities(entityType).par.foreach((id) => {
      getEntity(entityType, id) match {
        case Some(entity) => entities.put(id, entity)
        case _            =>
      }
    })
    entities
  }

  /**
    * Load the associated edges for the given VertexId
    * @param entityId
    * @return
    */
  override def getAssociatedEdges(entityId: Long) = ???

  /**
    * Load the history for the given $entityId
    * @param entityType
    * @param entityId
    * @return
    */
  override def getHistory(entityType: KeyEnum.Value, entityId: Long) : Nothing = ???

  /**
    * Get properties (and their history) for the given entityId
    * @param entityType
    * @param entityId
    * @return
    */
  override def getProperties(entityType: KeyEnum.Value, entityId: Long) = ???


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


  private def getVertex(entityId : Int) : Vertex = {
    var creationTime : Long = 0
    // Verify existence
    redis.get(KeyEnum.vertices, entityId) match {
      case None => throw EntityIdNotFoundException(entityId)
      case _ => // Some(id)
    }

    // get Creation Time
    redis.get(s"${KeyEnum.vertices}:$entityId:${SubKeyEnum.creationTime}") match {
      case None => throw CreationTimeNotFoundException(entityId)
      case Some(time) => creationTime = time.toLong
    }
    Vertex(creationTime = creationTime, vertexId = entityId,
      associatedEdges = getAssociatedEdges(entityId),
      previousState = getHistory(KeyEnum.vertices, entityId),
      properties = getProperties(KeyEnum.vertices, entityId)
    )
  }

  private def getEdge(edgeId : Long) : Edge = {
    var creationTime : Long = 0
    redis.get(KeyEnum.edges, edgeId) match {
      case None => throw EntityIdNotFoundException(edgeId)
      case _ =>
    }
    redis.get(s"${KeyEnum.edges}:$edgeId:${SubKeyEnum.creationTime}") match {
      case None => throw CreationTimeNotFoundException(edgeId)
      case Some(time) => creationTime = time.toLong
    }

    Edge(creationTime, edgeId,
      getHistory(KeyEnum.edges, edgeId),
      getProperties(KeyEnum.vertices, edgeId))
  }
}
