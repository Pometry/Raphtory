package com.raphtory.core.storage

import com.raphtory.core.model.graphentities._
import com.raphtory.core.utils.exceptions._
import com.raphtory.core.utils.{KeyEnum, SubKeyEnum, Utils}
import com.redis.RedisClient

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap

object RedisConnector extends ReaderConnector with WriterConnector {
  private val redis : RedisClient = new RedisClient("localhost", 6379)
  val routerID = -1 //fake router ID for entities brought back from storage
  /**
    * Add the entity and set its creation time (if it doesn't exist)
    * @param entityType
    * @param entityId
    * @param creationTime
    */
  override def addEntity(entityType : KeyEnum.Value, entityId : Long, creationTime : Long) = {
    redis.sadd(entityType, entityId)
    redis.sadd(creationTimeKey(entityType, entityId), creationTime)
  }

  /**
    * add a new change on the state of the Entity (history)
    * @param entityType
    * @param entityId
    * @param timestamp
    * @param value
    */
  override def addState(entityType : KeyEnum.Value, entityId : Long, timestamp : Long, value : Boolean) = {

    redis.set(historyKey(entityType, entityId, timestamp), value)
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
    redis.set(propertyKey(entityType, entityId, key, timestamp), value)
  }

  /**
    * add one associated vertex to the set of associatedEdges for the given Vertex
    * @param vertexId
    * @param edgeId
    */
  override def addAssociatedEdge(vertexId : Long, edgeId : Long) = {
    redis.sadd(associatedEdgesKey(vertexId), edgeId)
  }

  /**
    * check for the existence of the given $entityId in the $entityType set
    * @param entityType
    * @param entityId
    * @return
    */
  override def lookupEntity(entityType: KeyEnum.Value, entityId: Long) : Boolean = {
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
          case None    => Long.MaxValue // TODO check
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
    val entities = ParTrieMap[Long, Entity]()
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
    * @param vertexId
    * @return
    */
  override def getAssociatedEdges(vertexId: Long) : mutable.LinkedHashSet[Edge]= {
    val edges = mutable.LinkedHashSet[Edge]()
    redis.smembers(associatedEdgesKey(vertexId)) match {
      case Some(set) =>
        set.par.foreach {
          case Some(str) => edges.+(getEdge(str.toLong))
          case None =>
        }
      case None =>
    }
    edges
  }

  /**
    * Load the history for the given $entityId
    * @param entityType
    * @param entityId
    * @return
    */
  override def getHistory(entityType: KeyEnum.Value, entityId: Long) : mutable.TreeMap[Long, Boolean] = {
    val history = mutable.TreeMap[Long, Boolean]()
    redis.keys(s"$entityType:$entityId|${SubKeyEnum.history}:*")
      .get.par.foreach(
      optStr => history.put(optStr.get.split(":")(3).toLong, false)
    )
    history
  }

  /**
    * Get properties (and their history) for the given entityId
    * @param entityType
    * @param entityId
    * @return
    */
  override def getProperties(entityType: KeyEnum.Value, entityId: Long) : ParTrieMap[String, Property] = {
    val properties = ParTrieMap[String, Property]()
    redis.keys(s"$entityType:$entityId:*:*")
      .get.par.foreach(
      optStr => {
        val key = optStr.get
        val propertyName = key.split(":")(2)
        val propertyTime = key.split(":")(3).toLong
        val propertyValue = redis.get(optStr.get).get
        properties.get(propertyName) match {
          case Some(ep) => ep.update(propertyTime, propertyValue)
          case None => properties.put(propertyName, new Property(propertyTime, propertyName, propertyValue))
        }
      }
    )
    properties
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

  private def getVertex(entityId : Int) : Vertex = {
    var creationTime : Long = 0
    // Verify existence
    redis.get(KeyEnum.vertices, entityId) match {
      case None => throw EntityIdNotFoundException(entityId)
      case _ => // Some(id)
    }

    // get Creation Time
    redis.get(creationTimeKey(KeyEnum.vertices, entityId)) match {
      case None => throw CreationTimeNotFoundException(entityId)
      case Some(time) => creationTime = time.toLong
    }
    val v = Vertex(routerID, creationTime = creationTime, vertexId = entityId,
      associatedEdges = getAssociatedEdges(entityId),
      previousState = getHistory(KeyEnum.vertices, entityId),
      properties = getProperties(KeyEnum.vertices, entityId)
    )
    EntitiesStorage.vertices.synchronized {
      EntitiesStorage.vertices.get(entityId) match {
        case Some(vx) => return vx
        case None => EntitiesStorage.vertices.put(entityId, v)
      }
    }
    v
  }

  private def getEdge(edgeId : Long) : Edge = {
    var e : Edge = null
    var creationTime : Long = 0
    redis.get(KeyEnum.edges, edgeId) match {
      case None => throw EntityIdNotFoundException(edgeId)
      case _ =>
    }
    redis.get(creationTimeKey(KeyEnum.edges, edgeId)) match {
      case None => throw CreationTimeNotFoundException(edgeId)
      case Some(time) => creationTime = time.toLong
    }

    val srcId = Utils.getIndexHI(edgeId)
    val dstId = Utils.getIndexLO(edgeId)
    val remoteSrc = Utils.getPartition(Utils.getIndexHI(srcId), EntitiesStorage.managerCount) != EntitiesStorage.managerID
    val remoteDst = Utils.getPartition(Utils.getIndexLO(dstId), EntitiesStorage.managerCount) != EntitiesStorage.managerID

    if (remoteSrc || remoteDst) {
      var remotePos = RemotePos.Destination
      var remotePartitionId = Utils.getPartition(dstId, EntitiesStorage.managerCount)
      if (remoteSrc) {
        remotePos = RemotePos.Source
        remotePartitionId = Utils.getPartition(srcId, EntitiesStorage.managerCount)
      }
      // Remote edge
      e = RemoteEdge(routerID,creationTime, edgeId,
        getHistory(KeyEnum.edges, edgeId),
        getProperties(KeyEnum.edges, edgeId),
        remotePos,
        remotePartitionId
      )
    } else {
      e = Edge(routerID,creationTime, edgeId,
        getHistory(KeyEnum.edges, edgeId),
        getProperties(KeyEnum.edges, edgeId))
    }
    EntitiesStorage.synchronized {
      EntitiesStorage.edges.get(edgeId) match {
        case None => EntitiesStorage.edges.put(edgeId, e)
        case Some(edge) => return edge
      }
    }
    e
  }


  private def associatedEdgesKey(vertexId : Long) : String =
    s"${KeyEnum.vertices}:$vertexId:${KeyEnum.edges}"

  private def propertyKey(entityType: KeyEnum.Value, entityId: Long,
                          key: String, timestamp : Long) =
    s"$entityType:$entityId:$key:$timestamp"

  private def creationTimeKey(entityType : KeyEnum.Value, entityId : Long) =
    s"$entityType:$entityId:${SubKeyEnum.creationTime}"

  private def historyKey(entityType: KeyEnum.Value, entityId: Long, timestamp : Long) =
    s"$entityType:$entityId|${SubKeyEnum.history}:$timestamp"
}
