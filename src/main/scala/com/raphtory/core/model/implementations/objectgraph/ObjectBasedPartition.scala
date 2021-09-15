package com.raphtory.core.model.implementations.objectgraph

import com.raphtory.core.analysis.ObjectGraphLens
import com.raphtory.core.model.communication._
import com.raphtory.core.model.graph.{GraphPartition, GraphPerspective}
import com.raphtory.core.model.graph.visitor.Vertex
import com.raphtory.core.model.implementations.objectgraph.entities.internal.{RaphtoryEdge, RaphtoryEntity, RaphtoryVertex, SplitRaphtoryEdge}

import scala.collection.concurrent.TrieMap
import scala.collection.parallel.mutable.ParTrieMap

class ObjectBasedPartition(initManagerCount: Int, managerID: Int, workerID: Int) extends GraphPartition(initManagerCount: Int, managerID: Int, workerID: Int){
  /**
    * Map of vertices contained in the partition
    */


  val vertices = ParTrieMap[Long, RaphtoryVertex]()

  def addProperties(msgTime: Long, entity: RaphtoryEntity, properties: Properties): Unit =
    properties.property.foreach {
      case StringProperty(key, value) => entity + (msgTime, false, key, value)
      case LongProperty(key, value) => entity + (msgTime, false, key, value)
      case DoubleProperty(key, value) => entity + (msgTime, false, key, value)
      case FloatProperty(key, value) => entity + (msgTime, false, key, value)
      case ImmutableProperty(key, value) => entity + (msgTime, true, key, value)
    }
  // if the add come with some properties add all passed properties into the entity

  override def addVertex(msgTime: Long, srcId: Long, properties: Properties, vertexType: Option[Type]): Unit =
    addVertexInternal(msgTime,srcId, properties, vertexType)

  def addVertexInternal(msgTime: Long, srcId: Long, properties: Properties, vertexType: Option[Type]): RaphtoryVertex = { //Vertex add handler function
    val vertex: RaphtoryVertex = vertices.get(srcId) match { //check if the vertex exists
      case Some(v) => //if it does
        v revive msgTime //add the history point
        v
      case None => //if it does not exist
        val v = new RaphtoryVertex(msgTime, srcId, initialValue = true) //create a new vertex
        vertexCount.increment()
        v.setType(vertexType.map(_.name))
        vertices put(srcId, v) //put it in the map
        v
    }
    addProperties(msgTime, vertex, properties)

    vertex //return the vertex
  }

  def getVertexOrPlaceholder(msgTime: Long, id: Long): RaphtoryVertex =
    vertices.get(id) match {
      case Some(vertex) => vertex
      case None =>
        vertexCount.increment()
        val vertex = new RaphtoryVertex(msgTime, id, initialValue = true)
        vertices put(id, vertex)
        vertex wipe()
        vertex
    }


  def removeVertex(msgTime: Long, srcId: Long, channelId: String, channelTime: Int): List[TrackedGraphEffect[GraphUpdateEffect]] = {
    val vertex = vertices.get(srcId) match {
      case Some(v) =>
        v kill msgTime
        v
      case None => //if the removal has arrived before the creation
        vertexCount.increment()
        val v = new RaphtoryVertex(msgTime, srcId, initialValue = false) //create a placeholder
        vertices put(srcId, v) //add it to the map
        v
    }

    val messagesForIncoming = vertex.incomingEdges
      .map { edge =>
        edge._2 match {
          case remoteEdge: SplitRaphtoryEdge =>
            remoteEdge kill msgTime
            Some[TrackedGraphEffect[GraphUpdateEffect]](TrackedGraphEffect(channelId, channelTime, InboundEdgeRemovalViaVertex(msgTime, remoteEdge.getSrcId, remoteEdge.getDstId)))
          case edge => //if it is a local edge -- opperated by the same worker, therefore we can perform an action -- otherwise we must inform the other local worker to handle this
            edge kill msgTime
            None
        }
      }
      .toList
      .flatten
    val messagesForOutgoing = vertex.outgoingEdges
      .map { edge =>
        edge._2 match {
          case remoteEdge: SplitRaphtoryEdge =>
            remoteEdge kill msgTime //outgoing edge always opperated by the same worker, therefore we can perform an action
            Some[TrackedGraphEffect[GraphUpdateEffect]](TrackedGraphEffect(channelId, channelTime, OutboundEdgeRemovalViaVertex(msgTime, remoteEdge.getSrcId, remoteEdge.getDstId)))
          case edge =>
            edge kill msgTime //outgoing edge always opperated by the same worker, therefore we can perform an action
            None
        }
      }
      .toList
      .flatten
    val messages = messagesForIncoming ++ messagesForOutgoing
    if (messages.size != vertex.getEdgesRequringSync())
      logger.error(s"The number of Messages to sync [${messages.size}] does not match to system value [${vertex.getEdgesRequringSync()}]")
    messages
  }

  /**
    * Edges Methods
    */
  def addEdge(msgTime: Long, srcId: Long, dstId: Long, properties: Properties, edgeType: Option[Type], channelId: String, channelTime: Int): Option[TrackedGraphEffect[GraphUpdateEffect]] = {
    val local = checkDst(dstId, managerCount, managerID,workerID) //is the dst on this machine
    val srcVertex = addVertexInternal(msgTime, srcId, Properties(), None) // create or revive the source ID

    val (present, edge) = srcVertex.getOutgoingEdge(dstId) match {
      case Some(e) => //retrieve the edge if it exists
        (true, e)
      case None => //if it does not
        val newEdge = if (local) {
          val created = new RaphtoryEdge(workerID, msgTime, srcId, dstId, initialValue = true) //create the new edge, local or remote
          localEdgeCount.increment()
          created
        } else {
          val created = new SplitRaphtoryEdge(workerID, msgTime, srcId, dstId, initialValue = true)
          masterSplitEdgeCount.increment()
          created
        }
        newEdge.setType(edgeType.map(_.name))
        srcVertex.addOutgoingEdge(newEdge) //add this edge to the vertex
        (false, newEdge)
    }

    val maybeEffect: Option[TrackedGraphEffect[GraphUpdateEffect]] =
      if (present) {
        edge revive msgTime //if the edge was previously created we need to revive it
        if (local) {
          if (srcId != dstId) {
            addVertexInternal(msgTime, dstId, Properties(), None) // do the same for the destination ID
          }
          None
        }
        else
          Some(TrackedGraphEffect(channelId, channelTime, SyncExistingEdgeAdd(msgTime, srcId, dstId, properties))) // inform the partition dealing with the destination node*/
      }
      else {
        val deaths = srcVertex.removeList //we extract the removals from the src
        edge killList deaths // add them to the edge
        if (local) {
          if (srcId != dstId) {
            val dstVertex = addVertexInternal(msgTime, dstId, Properties(), None) // do the same for the destination ID
            dstVertex addIncomingEdge (edge) // add it to the dst as would not have been seen
            edge killList dstVertex.removeList //add the dst removes into the edge
          }
          else
            srcVertex addIncomingEdge (edge) // a self loop should be in the incoming map as well
          None
        }
        else {
          srcVertex.incrementEdgesRequiringSync() //if its not fully local and is new then increment the count for edges requireing a watermark count
          Some(TrackedGraphEffect(channelId, channelTime, SyncNewEdgeAdd(msgTime, srcId, dstId, properties, deaths, edgeType)))
        }
      }
    addProperties(msgTime, edge, properties)
    maybeEffect
  }

  def syncNewEdgeAdd(msgTime: Long, srcId: Long, dstId: Long, properties: Properties, srcRemovals: List[Long], edgeType: Option[Type], channelId: String, channelTime: Int): TrackedGraphEffect[GraphUpdateEffect] = {
    val dstVertex = addVertexInternal(msgTime, dstId, Properties(), None) //create or revive the destination node
    val edge = new SplitRaphtoryEdge(workerID, msgTime, srcId, dstId, initialValue = true)
    copySplitEdgeCount.increment()
    dstVertex addIncomingEdge (edge) //add the edge to the associated edges of the destination node
    val deaths = dstVertex.removeList //get the destination node deaths
    edge killList srcRemovals //pass source node death lists to the edge
    edge killList deaths // pass destination node death lists to the edge

    addProperties(msgTime, edge, properties)
    dstVertex.incrementEdgesRequiringSync()
    edge.setType(edgeType.map(_.name))
    TrackedGraphEffect(channelId, channelTime, SyncExistingRemovals(msgTime, srcId, dstId, deaths))
  }

  def syncExistingEdgeAdd(msgTime: Long, srcId: Long, dstId: Long, properties: Properties, channelId: String, channelTime: Int): TrackedGraphEffect[GraphUpdateEffect] = {
    val dstVertex = addVertexInternal(msgTime, dstId, Properties(), None) // revive the destination node
    dstVertex.getIncomingEdge(srcId) match {
      case Some(edge) =>
        edge revive msgTime //revive the edge
        addProperties(msgTime, edge, properties)
      case None => /*todo should this happen */
    }
    TrackedGraphEffect(channelId, channelTime, EdgeSyncAck(msgTime, srcId))
  }

  def removeEdge(msgTime: Long, srcId: Long, dstId: Long, channelId: String, channelTime: Int): Option[TrackedGraphEffect[GraphUpdateEffect]] = {
    val local = checkDst(dstId, managerCount, managerID,workerID)
    val srcVertex: RaphtoryVertex = getVertexOrPlaceholder(msgTime, srcId)

    val (present, edge) = srcVertex.getOutgoingEdge(dstId) match {
      case Some(e) =>
        (true, e)
      case None =>
        val newEdge = if (local) {
          localEdgeCount.increment()
          new RaphtoryEdge(workerID, msgTime, srcId, dstId, initialValue = false)
        } else {
          masterSplitEdgeCount.increment()
          new SplitRaphtoryEdge(workerID, msgTime, srcId, dstId, initialValue = false)
        }
        srcVertex.addOutgoingEdge(newEdge) // add the edge to the associated edges of the source node
        (false, newEdge)
    }

    if (present) {
      edge kill msgTime
      if (local)
          None
      else
        Some(TrackedGraphEffect(channelId, channelTime, SyncExistingEdgeRemoval(msgTime, srcId, dstId))) // inform the partition dealing with the destination node
    }
    else {
      val deaths = srcVertex.removeList
      edge killList deaths
      if (local){
          if (srcId != dstId) {
            val dstVertex = getVertexOrPlaceholder(msgTime, dstId) // do the same for the destination ID
            dstVertex addIncomingEdge (edge) // do the same for the destination node
            edge killList dstVertex.removeList //add the dst removes into the edge
          }
          None
      }
      else {
        srcVertex.incrementEdgesRequiringSync() //if its not fully local and is new then increment the count for edges requireing a watermark count
        Some(TrackedGraphEffect(channelId, channelTime, SyncNewEdgeRemoval(msgTime, srcId, dstId, deaths)))
      }
    }
  }

  def inboundEdgeRemovalViaVertex(msgTime: Long, srcId: Long, dstId: Long, channelId: String, channelTime: Int): TrackedGraphEffect[GraphUpdateEffect] = { //for the source getting an update about deletions from a remote worker
    getVertexOrPlaceholder(msgTime, srcId).getOutgoingEdge(dstId) match {
      case Some(edge) => edge kill msgTime
      case None =>
    }
    TrackedGraphEffect(channelId, channelTime, VertexRemoveSyncAck(msgTime, dstId))
  }

  def syncExistingEdgeRemoval(msgTime: Long, srcId: Long, dstId: Long, channelId: String, channelTime: Int): TrackedGraphEffect[GraphUpdateEffect] = {
    getVertexOrPlaceholder(msgTime, dstId).getIncomingEdge(srcId) match {
      case Some(e) => e kill msgTime
      case None => //logger.info(s"Worker ID $workerID Manager ID $managerID: remoteEdgeRemoval with no incoming edge")
    }
    TrackedGraphEffect(channelId, channelTime, EdgeSyncAck(msgTime, srcId))
  }

  def outboundEdgeRemovalViaVertex(msgTime: Long, srcId: Long, dstId: Long, channelId: String, channelTime: Int): TrackedGraphEffect[GraphUpdateEffect] = {
    getVertexOrPlaceholder(msgTime, dstId).getIncomingEdge(srcId) match {
      case Some(e) => e kill msgTime
      case None => //logger.info(s"Worker ID $workerID Manager ID $managerID: remoteEdgeRemovalFromVertex with no incoming edge")
    }
    TrackedGraphEffect(channelId, channelTime, VertexRemoveSyncAck(msgTime, srcId))
  }

  def syncNewEdgeRemoval(msgTime: Long, srcId: Long, dstId: Long, srcRemovals: List[Long], channelId: String, channelTime: Int): TrackedGraphEffect[GraphUpdateEffect] = {
    val dstVertex = getVertexOrPlaceholder(msgTime, dstId)
    dstVertex.incrementEdgesRequiringSync()
    copySplitEdgeCount.increment()
    val edge = new SplitRaphtoryEdge(workerID, msgTime, srcId, dstId, initialValue = false)
    dstVertex addIncomingEdge (edge) //add the edge to the destination nodes associated list
    val deaths = dstVertex.removeList //get the destination node deaths
    edge killList srcRemovals //pass source node death lists to the edge
    edge killList deaths // pass destination node death lists to the edge
    TrackedGraphEffect(channelId, channelTime, SyncExistingRemovals(msgTime, srcId, dstId, deaths))
  }

  def syncExistingRemovals(msgTime: Long, srcId: Long, dstId: Long, dstRemovals: List[Long]): Unit =
  //logger.info(s"Received deaths for $srcId --> $dstId from ${getManager(dstId, managerCount)}")
    getVertexOrPlaceholder(msgTime, srcId).getOutgoingEdge(dstId) match {
      case Some(edge) => edge killList dstRemovals
      case None => /*todo Should this happen*/
    }

  /**
    * Analysis Functions
    * */
  override def getVertices(perspective:GraphPerspective, time: Long, window: Long): TrieMap[Long, Vertex] = {
    val lens = perspective.asInstanceOf[ObjectGraphLens]
    vertices.map(v => (v._1,v._2.viewAt(time,lens).asInstanceOf[Vertex])).seq
  }
}
