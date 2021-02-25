package com.raphtory.core.model

import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.model.communication._
import com.raphtory.core.model.graphentities.{Edge, Entity, SplitEdge, Vertex}
import com.typesafe.scalalogging.LazyLogging
import kamon.Kamon

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap

/**
  * Singleton representing the Storage for the entities
  */
//TODO add capacity function based on memory used and number of updates processed/stored in memory
//TODO What happens when an edge which has been archived gets readded

final case class EntityStorage(initManagerCount: Int, managerID: Int, workerID: Int) extends LazyLogging {

  /**
    * Map of vertices contained in the partition
    */
  val vertices = ParTrieMap[Long, Vertex]()

  var managerCount: Int  = initManagerCount
  //stuff for compression and archiving
  var oldestTime: Long       = Long.MaxValue
  var newestTime: Long       = 0
  var windowTime: Long       = 0

  val vertexCount          = Kamon.counter("Raphtory_Vertex_Count").withTag("actor",s"PartitionWriter_$managerID").withTag("ID",workerID)
  val localEdgeCount       = Kamon.counter("Raphtory_Local_Edge_Count").withTag("actor",s"PartitionWriter_$managerID").withTag("ID",workerID)
  val copySplitEdgeCount   = Kamon.counter("Raphtory_Copy_Split_Edge_Count").withTag("actor",s"PartitionWriter_$managerID").withTag("ID",workerID)
  val masterSplitEdgeCount = Kamon.counter("Raphtory_Master_Split_Edge_Count").withTag("actor",s"PartitionWriter_$managerID").withTag("ID",workerID)


  def timings(updateTime: Long) = {
    if (updateTime < oldestTime && updateTime > 0) oldestTime = updateTime
    if (updateTime > newestTime)
      newestTime = updateTime //this isn't thread safe, but is only an approx for the archiving
  }

  def setManagerCount(count: Int) = this.managerCount = count

  def addProperties(msgTime: Long, entity: Entity, properties: Properties): Unit =
      properties.property.foreach {
        case StringProperty(key, value)    => entity + (msgTime, false, key, value)
        case LongProperty(key, value)      => entity + (msgTime, false, key, value)
        case DoubleProperty(key, value)    => entity + (msgTime, false, key, value)
        case ImmutableProperty(key, value) => entity + (msgTime, true, key, value)
      }
  // if the add come with some properties add all passed properties into the entity

  def addVertex(msgTime: Long, srcId: Long, properties: Properties, vertexType: Option[Type]): Vertex = { //Vertex add handler function
    val vertex: Vertex = vertices.get(srcId) match { //check if the vertex exists
      case Some(v) => //if it does
        v revive msgTime //add the history point
        v
      case None => //if it does not exist
        val v = new Vertex(msgTime, srcId, initialValue = true) //create a new vertex
        vertexCount.increment()
        vertexType.foreach(vType => v.setType(vType.name))
        vertices put (srcId, v) //put it in the map
        v
    }
    addProperties(msgTime, vertex, properties)

    vertex //return the vertex
  }

  def getVertexOrPlaceholder(msgTime: Long, id: Long): Vertex =
    vertices.get(id) match {
      case Some(vertex) => vertex
      case None =>
        vertexCount.increment()
        val vertex = new Vertex(msgTime, id, initialValue = true)
        vertices put (id, vertex)
        vertex wipe ()
        vertex
    }

  def vertexWorkerRequest(msgTime: Long, dstID: Long, srcID: Long, edge: Edge, present: Boolean,routerID:String,routerTime:Int): GraphEffect = {
    val dstVertex = addVertex(msgTime, dstID, Properties(), None) //if the worker creating an edge does not deal with the destination
    if (!present) {
      dstVertex.incrementEdgesRequiringSync()
      dstVertex addIncomingEdge edge // do the same for the destination node
      DstResponseFromOtherWorker(msgTime, srcID, dstID, dstVertex.removeList, routerID, routerTime)
    }
    else
      EdgeSyncAck(msgTime, srcID, routerID, routerTime)
  }

  def vertexWipeWorkerRequest(msgTime: Long, dstID: Long, srcID: Long, edge: Edge, present: Boolean,routerID:String,routerTime:Int): GraphEffect = {
    val dstVertex = getVertexOrPlaceholder(msgTime, dstID) // if the worker creating an edge does not deal with do the same for the destination ID
    if (!present) {
      dstVertex.incrementEdgesRequiringSync()
      dstVertex addIncomingEdge edge // do the same for the destination node
      DstResponseFromOtherWorker(msgTime, srcID, dstID, dstVertex.removeList,routerID,routerTime)
    }
    else
      EdgeSyncAck(msgTime, srcID, routerID, routerTime)
  }

  def vertexWorkerRequestEdgeHandler(
      msgTime: Long,
      srcID: Long,
      dstID: Long,
      removeList: mutable.TreeMap[Long, Boolean]
  ): Unit =
    getVertexOrPlaceholder(msgTime, srcID).getOutgoingEdge(dstID) match {
      case Some(edge) => edge killList removeList //add the dst removes into the edge
      case None       => logger.error(s"no edge from $srcID to $dstID")
    }

  def removeVertex(msgTime: Long, srcId: Long, channelId: String, channelTime: Int): List[GraphEffect] = {
    val vertex = vertices.get(srcId) match {
      case Some(v) =>
        v kill msgTime
        v
      case None => //if the removal has arrived before the creation
        vertexCount.increment()
        val v = new Vertex(msgTime, srcId, initialValue = false) //create a placeholder
        vertices put (srcId, v) //add it to the map
        v
    }

    val messagesForIncoming = vertex.incomingEdges.map(edge => {
      edge._2 match {
        case remoteEdge: SplitEdge =>
          remoteEdge kill msgTime
          Some(ReturnEdgeRemoval(msgTime, remoteEdge.getSrcId, remoteEdge.getDstId, channelId, channelTime))
        case edge => //if it is a local edge -- opperated by the same worker, therefore we can perform an action -- otherwise we must inform the other local worker to handle this
          if (edge.getWorkerID == workerID) {
            edge kill msgTime
            None
          }
          else {
            Some(EdgeRemoveForOtherWorker(msgTime, edge.getSrcId, edge.getDstId, channelId, channelTime))
          }
      }
    })
    val messagesForOutgoing = vertex.outgoingEdges.map (edge=>{
      edge._2 match {
        case remoteEdge: SplitEdge =>
          remoteEdge kill msgTime //outgoing edge always opperated by the same worker, therefore we can perform an action
          Some(RemoteEdgeRemovalFromVertex(msgTime, remoteEdge.getSrcId, remoteEdge.getDstId, channelId, channelTime))
        case edge =>
          edge kill msgTime //outgoing edge always opperated by the same worker, therefore we can perform an action
          None
      }
    })
    val messages = (messagesForIncoming.flatten ++ messagesForOutgoing.flatten).toList
    if(messages.size != vertex.getEdgesRequringSync())
      logger.error(s"The number of Messages to sync [${messages.size}] does not match to system value [${vertex.getEdgesRequringSync()}]")
    messages
  }

  /**
    * Edges Methods
    */
  def addEdge(msgTime: Long, srcId: Long, dstId: Long, channelId:String, channelTime:Int, properties: Properties, edgeType: Option[Type]): Option[GraphEffect] = {
    val local      = checkDst(dstId, managerCount, managerID)     //is the dst on this machine
    val sameWorker = checkWorker(dstId, managerCount, workerID)   // is the dst handled by the same worker
    val srcVertex  = addVertex(msgTime, srcId, Properties(), None) // create or revive the source ID

    val (present, edge) = srcVertex.getOutgoingEdge(dstId) match {
      case Some(e) => //retrieve the edge if it exists
        (true, e)
      case None => //if it does not
        val newEdge = if (local) {
          val created = new Edge(workerID, msgTime, srcId, dstId, initialValue = true) //create the new edge, local or remote
          localEdgeCount.increment()
          created
        } else {
          val created = new SplitEdge(workerID, msgTime, srcId, dstId, initialValue = true)
          masterSplitEdgeCount.increment()
          created
        }
        edgeType.foreach(eType => newEdge.setType(eType.name))
        srcVertex.addOutgoingEdge(newEdge) //add this edge to the vertex
        (false, newEdge)
    }

    val maybeEffect: Option[GraphEffect] = if (present) {
      edge revive msgTime //if the edge was previously created we need to revive it
      if (local) {
        if (sameWorker) {
          if (srcId != dstId) {
            addVertex(msgTime, dstId, Properties(), None) // do the same for the destination ID

          }
          None
        } else {
          Some(DstAddForOtherWorker(msgTime, dstId, srcId, edge, present, channelId, channelTime))
        }
      } else {
        Some(RemoteEdgeAdd(msgTime, srcId, dstId, properties, edgeType.orNull, channelId, channelTime)) // inform the partition dealing with the destination node*/
      }
    } else {
      val deaths = srcVertex.removeList //we extract the removals from the src
      edge killList deaths // add them to the edge
      if (local) {
        if (sameWorker) {
          if (srcId != dstId) {
            val dstVertex = addVertex(msgTime, dstId, Properties(), None) // do the same for the destination ID
            dstVertex addIncomingEdge (edge) // add it to the dst as would not have been seen
            edge killList dstVertex.removeList //add the dst removes into the edge
          }
          else {
            srcVertex addIncomingEdge (edge)
          } // a self loop should be in the incoming map as well
          None
        } else {
          Some(DstAddForOtherWorker(msgTime, dstId, srcId, edge, present, channelId, channelTime))
        }
      } else {
        srcVertex.incrementEdgesRequiringSync() //if its not fully local and is new then increment the count for edges requireing a watermark count
        Some(RemoteEdgeAddNew(msgTime, srcId, dstId, properties, deaths, edgeType.orNull, channelId: String, channelTime))
      }
    }
    addProperties(msgTime, edge, properties)

    maybeEffect
  }

  def remoteEdgeAddNew(
      msgTime: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      srcDeaths: mutable.TreeMap[Long, Boolean],
      edgeType: Type,
      routerID:String,
      routerTime:Int,
  ): GraphEffect = {
    val dstVertex = addVertex(msgTime, dstId, Properties(), None) //create or revive the destination node
    val edge = new SplitEdge(workerID, msgTime, srcId, dstId, initialValue = true)
    copySplitEdgeCount.increment()
    dstVertex addIncomingEdge (edge) //add the edge to the associated edges of the destination node
    val deaths = dstVertex.removeList //get the destination node deaths
    edge killList srcDeaths //pass source node death lists to the edge
    edge killList deaths    // pass destination node death lists to the edge
    if(properties!=null)
      addProperties(msgTime, edge, properties)
    dstVertex.incrementEdgesRequiringSync()
    if (!(edgeType == null)) edge.setType(edgeType.name)
    RemoteReturnDeaths(msgTime, srcId, dstId, deaths, routerID, routerTime)
  }

  def remoteEdgeAdd(msgTime: Long, srcId: Long, dstId: Long, properties: Properties = null, edgeType: Type, routerID:String, routerTime:Int): GraphEffect = {
    val dstVertex = addVertex(msgTime, dstId, Properties(), None) // revive the destination node
    dstVertex.getIncomingEdge(srcId) match {
      case Some(edge) =>
        edge revive msgTime //revive the edge
        if(properties!=null)
          addProperties(msgTime, edge, properties)
      case None => /*todo should this happen */
    }
    EdgeSyncAck(msgTime, srcId, routerID, routerTime)
  }

  def removeEdge(msgTime: Long, srcId: Long, dstId: Long, channelId: String, channelTime: Int): Option[GraphEffect] = {
    val local      = checkDst(dstId, managerCount, managerID)
    val sameWorker = checkWorker(dstId, managerCount, workerID) // is the dst handled by the same worker

    val srcVertex: Vertex = getVertexOrPlaceholder(msgTime, srcId)

    val (present, edge) = srcVertex.getOutgoingEdge(dstId) match {
      case Some(e) =>
        (true, e)
      case None =>
        val newEdge = if (local) {
          localEdgeCount.increment()
          new Edge(workerID, msgTime, srcId, dstId, initialValue = false)
        } else {
          masterSplitEdgeCount.increment()
          new SplitEdge(workerID, msgTime, srcId, dstId, initialValue = false)
        }
        srcVertex.addOutgoingEdge(newEdge) // add the edge to the associated edges of the source node
        (false, newEdge)
    }

    if(present) {
      edge kill msgTime
      if (local) {
        if (sameWorker) {
          None
        } else { // if it is a different worker, ask that other worker to complete the dst part of the edge
          Some(DstWipeForOtherWorker(msgTime, dstId, srcId, edge, present, channelId, channelTime))
        }
      } else {
        Some(RemoteEdgeRemoval(msgTime, srcId, dstId, channelId, channelTime)) // inform the partition dealing with the destination node
      }
    } else {
      val deaths = srcVertex.removeList
      edge killList deaths
      if (local) {
        if (sameWorker) {
          if (srcId != dstId) {
            val dstVertex = getVertexOrPlaceholder(msgTime, dstId) // do the same for the destination ID
            dstVertex addIncomingEdge (edge)   // do the same for the destination node
            edge killList dstVertex.removeList //add the dst removes into the edge
          }
          None
        } else { // if it is a different worker, ask that other worker to complete the dst part of the edge
          Some(DstWipeForOtherWorker(msgTime, dstId, srcId, edge, present, channelId, channelTime))
        }
      } else {
        srcVertex.incrementEdgesRequiringSync() //if its not fully local and is new then increment the count for edges requireing a watermark count
        Some(RemoteEdgeRemovalNew(msgTime, srcId, dstId, deaths, channelId, channelTime))
      }
    }
  }

  def returnEdgeRemoval(msgTime: Long, srcId: Long, dstId: Long,routerID:String,routerTime:Int): GraphEffect = { //for the source getting an update about deletions from a remote worker
    getVertexOrPlaceholder(msgTime, srcId).getOutgoingEdge(dstId) match {
      case Some(edge) => edge kill msgTime
      case None       => //todo should this happen
    }
    VertexRemoveSyncAck(msgTime, dstId, routerID, routerTime)
  }

  def edgeRemovalFromOtherWorker(msgTime: Long, srcID: Long, dstID: Long,routerID:String,routerTime:Int): GraphEffect = {
    getVertexOrPlaceholder(msgTime, srcID).getOutgoingEdge(dstID) match {
      case Some(edge) => edge kill msgTime
      case None       => //todo should this happen?
    }
    VertexRemoveSyncAck(msgTime, dstID, routerID, routerTime)
  }

  def remoteEdgeRemoval(msgTime: Long, srcId: Long, dstId: Long,routerID:String,routerTime:Int): GraphEffect = {
    val dstVertex = getVertexOrPlaceholder(msgTime, dstId)
    dstVertex.getIncomingEdge(srcId) match {
      case Some(e) => e kill msgTime
      case None    => //logger.info(s"Worker ID $workerID Manager ID $managerID: remoteEdgeRemoval with no incoming edge")
    }
    EdgeSyncAck(msgTime, srcId, routerID, routerTime)
  }
  def remoteEdgeRemovalFromVertex(msgTime: Long, srcId: Long, dstId: Long,routerID:String,routerTime:Int): GraphEffect = {
    val dstVertex = getVertexOrPlaceholder(msgTime, dstId)
    dstVertex.getIncomingEdge(srcId) match {
      case Some(e) => e kill msgTime
      case None    => //logger.info(s"Worker ID $workerID Manager ID $managerID: remoteEdgeRemovalFromVertex with no incoming edge")
    }
    VertexRemoveSyncAck(msgTime, srcId, routerID, routerTime)
  }

  def remoteEdgeRemovalNew(msgTime: Long, srcId: Long, dstId: Long, srcDeaths: mutable.TreeMap[Long, Boolean],routerID:String,routerTime:Int): GraphEffect = {
    val dstVertex = getVertexOrPlaceholder(msgTime, dstId)
    dstVertex.incrementEdgesRequiringSync()
    copySplitEdgeCount.increment()
    val edge = new SplitEdge(workerID, msgTime, srcId, dstId, initialValue = false)
    dstVertex addIncomingEdge (edge) //add the edge to the destination nodes associated list
    val deaths = dstVertex.removeList //get the destination node deaths
    edge killList srcDeaths //pass source node death lists to the edge
    edge killList deaths    // pass destination node death lists to the edge
    RemoteReturnDeaths(msgTime, srcId, dstId, deaths,routerID,routerTime)
  }

  def remoteReturnDeaths(msgTime: Long, srcId: Long, dstId: Long, dstDeaths: mutable.TreeMap[Long, Boolean]): Unit = {
    //logger.info(s"Received deaths for $srcId --> $dstId from ${getManager(dstId, managerCount)}")
    getVertexOrPlaceholder(msgTime, srcId).getOutgoingEdge(dstId) match {
      case Some(edge) => edge killList dstDeaths
      case None       => /*todo Should this happen*/
    }
  }

  //TODO these are placed here until YanYangs changes can be integrated
  def getManager(srcId: Long, managerCount: Int): String = {
    val mod     = srcId.abs % (managerCount * 10)
    val manager = mod / 10
    val worker  = mod % 10
    s"/user/Manager_${manager}_child_$worker"
  }
  def checkDst(dstID: Long, managerCount: Int, managerID: Int): Boolean = ((dstID.abs % (managerCount * 10)) / 10).toInt == managerID //check if destination is also local
  def checkWorker(dstID: Long, managerCount: Int, workerID: Int): Boolean = ((dstID.abs % (managerCount * 10)) % 10).toInt == workerID //check if destination is also local
}
