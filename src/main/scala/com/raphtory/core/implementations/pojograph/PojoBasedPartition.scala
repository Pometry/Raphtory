package com.raphtory.core.implementations.pojograph

import com.raphtory.core.components.akkamanagement.RaphtoryActor
import com.raphtory.core.implementations.pojograph.entities.internal.{PojoEdge, PojoEntity, PojoVertex, SplitEdge}
import com.raphtory.core.model.graph.{DoubleProperty, EdgeSyncAck, FloatProperty, GraphLens, GraphPartition, GraphUpdateEffect, ImmutableProperty, InboundEdgeRemovalViaVertex, LongProperty, OutboundEdgeRemovalViaVertex, Properties, StringProperty, SyncExistingEdgeAdd, SyncExistingEdgeRemoval, SyncExistingRemovals, SyncNewEdgeAdd, SyncNewEdgeRemoval, TrackedGraphEffect, Type, VertexRemoveSyncAck}
import com.raphtory.core.model.graph.visitor.Vertex

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

class PojoBasedPartition(partition: Int) extends GraphPartition(partition: Int){
  /**
    * Map of vertices contained in the partition
    */


  val vertices = mutable.Map[Long, PojoVertex]()

  def addProperties(msgTime: Long, entity: PojoEntity, properties: Properties): Unit =
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

  def addVertexInternal(msgTime: Long, srcId: Long, properties: Properties, vertexType: Option[Type]): PojoVertex = { //Vertex add handler function
    val vertex: PojoVertex = vertices.get(srcId) match { //check if the vertex exists
      case Some(v) => //if it does
        v revive msgTime //add the history point
        v
      case None => //if it does not exist
        val v = new PojoVertex(msgTime, srcId, initialValue = true) //create a new vertex
        vertices.+=((srcId, v))//put it in the map)
        v.setType(vertexType.map(_.name))
        v
    }
    addProperties(msgTime, vertex, properties)

    vertex //return the vertex
  }

  def getVertexOrPlaceholder(msgTime: Long, id: Long): PojoVertex =
    vertices.get(id) match {
      case Some(vertex) => vertex
      case None =>
        val vertex = new PojoVertex(msgTime, id, initialValue = true)
        vertices put(id, vertex)
        vertex wipe()
        vertex
    }


  def removeVertex(msgTime: Long, srcId: Long, channelId: Int, channelTime: Int): List[TrackedGraphEffect[GraphUpdateEffect]] = {
    val vertex = vertices.get(srcId) match {
      case Some(v) =>
        v kill msgTime
        v
      case None => //if the removal has arrived before the creation
        val v = new PojoVertex(msgTime, srcId, initialValue = false) //create a placeholder
        vertices put(srcId, v) //add it to the map
        v
    }

    val messagesForIncoming = vertex.incomingEdges
      .map { edge =>
        edge._2 match {
          case remoteEdge: SplitEdge =>
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
          case remoteEdge: SplitEdge =>
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
    //if (messages.size != vertex.getEdgesRequringSync())
    //  logger.error(s"The number of Messages to sync [${messages.size}] does not match to system value [${vertex.getEdgesRequringSync()}]")
    messages
  }

  /**
    * Edges Methods
    */
  def addEdge(msgTime: Long, srcId: Long, dstId: Long, properties: Properties, edgeType: Option[Type], channelId: Int, channelTime: Int): Option[TrackedGraphEffect[GraphUpdateEffect]] = {
    val local = checkDst(dstId) //is the dst on this machine
    val srcVertex = addVertexInternal(msgTime, srcId, Properties(), None) // create or revive the source ID

    val (present, edge) = srcVertex.getOutgoingEdge(dstId) match {
      case Some(e) => //retrieve the edge if it exists
        (true, e)
      case None => //if it does not
        val newEdge = if (local) {
          new PojoEdge(msgTime, srcId, dstId, initialValue = true) //create the new edge, local or remote
        } else {
          new SplitEdge(msgTime, srcId, dstId, initialValue = true)
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
        val deaths = if(RaphtoryActor.hasDeletions) {
          val list = srcVertex.removeList
          edge killList list // add them to the edge
          list
        } else List() //we extract the removals from the src

        if (local) {
          if (srcId != dstId) {
            val dstVertex = addVertexInternal(msgTime, dstId, Properties(), None) // do the same for the destination ID
            dstVertex addIncomingEdge (edge) // add it to the dst as would not have been seen
            if(RaphtoryActor.hasDeletions) edge killList dstVertex.removeList //add the dst removes into the edge
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

  def syncNewEdgeAdd(msgTime: Long, srcId: Long, dstId: Long, properties: Properties, srcRemovals: List[Long], edgeType: Option[Type], channelId: Int, channelTime: Int): TrackedGraphEffect[GraphUpdateEffect] = {
    val dstVertex = addVertexInternal(msgTime, dstId, Properties(), None) //create or revive the destination node
    val edge = new SplitEdge(msgTime, srcId, dstId, initialValue = true)
    dstVertex addIncomingEdge (edge) //add the edge to the associated edges of the destination node
    val deaths = if(RaphtoryActor.hasDeletions) {
      val list = dstVertex.removeList
      edge killList srcRemovals //pass source node death lists to the edge
      edge killList list // pass destination node death lists to the edge
      list
    } else List() //get the destination node deaths


    addProperties(msgTime, edge, properties)
    dstVertex.incrementEdgesRequiringSync()
    edge.setType(edgeType.map(_.name))
    TrackedGraphEffect(channelId, channelTime, SyncExistingRemovals(msgTime, srcId, dstId, deaths))
  }

  def syncExistingEdgeAdd(msgTime: Long, srcId: Long, dstId: Long, properties: Properties, channelId: Int, channelTime: Int): TrackedGraphEffect[GraphUpdateEffect] = {
    val dstVertex = addVertexInternal(msgTime, dstId, Properties(), None) // revive the destination node
    dstVertex.getIncomingEdge(srcId) match {
      case Some(edge) =>
        edge revive msgTime //revive the edge
        addProperties(msgTime, edge, properties)
      case None => /*todo should this happen */
    }
    TrackedGraphEffect(channelId, channelTime, EdgeSyncAck(msgTime, srcId))
  }

  def removeEdge(msgTime: Long, srcId: Long, dstId: Long, channelId: Int, channelTime: Int): Option[TrackedGraphEffect[GraphUpdateEffect]] = {
    val local = checkDst(dstId)
    val srcVertex: PojoVertex = getVertexOrPlaceholder(msgTime, srcId)

    val (present, edge) = srcVertex.getOutgoingEdge(dstId) match {
      case Some(e) =>
        (true, e)
      case None =>
        val newEdge = if (local) {
          new PojoEdge(msgTime, srcId, dstId, initialValue = false)
        } else {
          new SplitEdge(msgTime, srcId, dstId, initialValue = false)
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
      val deaths = if(RaphtoryActor.hasDeletions) {
        val list = srcVertex.removeList
        edge killList list
        list
      } else List()

      if (local){
          if (srcId != dstId) {
            val dstVertex = getVertexOrPlaceholder(msgTime, dstId) // do the same for the destination ID
            dstVertex addIncomingEdge (edge) // do the same for the destination node
            if(RaphtoryActor.hasDeletions) edge killList dstVertex.removeList //add the dst removes into the edge
          }
          None
      }
      else {
        srcVertex.incrementEdgesRequiringSync() //if its not fully local and is new then increment the count for edges requireing a watermark count
        Some(TrackedGraphEffect(channelId, channelTime, SyncNewEdgeRemoval(msgTime, srcId, dstId, deaths)))
      }
    }
  }

  def inboundEdgeRemovalViaVertex(msgTime: Long, srcId: Long, dstId: Long, channelId: Int, channelTime: Int): TrackedGraphEffect[GraphUpdateEffect] = { //for the source getting an update about deletions from a remote worker
    getVertexOrPlaceholder(msgTime, srcId).getOutgoingEdge(dstId) match {
      case Some(edge) => edge kill msgTime
      case None =>
    }
    TrackedGraphEffect(channelId, channelTime, VertexRemoveSyncAck(msgTime, dstId))
  }

  def syncExistingEdgeRemoval(msgTime: Long, srcId: Long, dstId: Long, channelId: Int, channelTime: Int): TrackedGraphEffect[GraphUpdateEffect] = {
    getVertexOrPlaceholder(msgTime, dstId).getIncomingEdge(srcId) match {
      case Some(e) => e kill msgTime
      case None => //logger.info(s"Worker ID $workerID Manager ID $managerID: remoteEdgeRemoval with no incoming edge")
    }
    TrackedGraphEffect(channelId, channelTime, EdgeSyncAck(msgTime, srcId))
  }

  def outboundEdgeRemovalViaVertex(msgTime: Long, srcId: Long, dstId: Long, channelId: Int, channelTime: Int): TrackedGraphEffect[GraphUpdateEffect] = {
    getVertexOrPlaceholder(msgTime, dstId).getIncomingEdge(srcId) match {
      case Some(e) => e kill msgTime
      case None => //logger.info(s"Worker ID $workerID Manager ID $managerID: remoteEdgeRemovalFromVertex with no incoming edge")
    }
    TrackedGraphEffect(channelId, channelTime, VertexRemoveSyncAck(msgTime, srcId))
  }

  def syncNewEdgeRemoval(msgTime: Long, srcId: Long, dstId: Long, srcRemovals: List[Long], channelId: Int, channelTime: Int): TrackedGraphEffect[GraphUpdateEffect] = {
    val dstVertex = getVertexOrPlaceholder(msgTime, dstId)
    dstVertex.incrementEdgesRequiringSync()
    val edge = new SplitEdge(msgTime, srcId, dstId, initialValue = false)
    dstVertex addIncomingEdge (edge) //add the edge to the destination nodes associated list
    val deaths = if(RaphtoryActor.hasDeletions) {
      val list = dstVertex.removeList
      edge killList srcRemovals //pass source node death lists to the edge
      edge killList list // pass destination node death lists to the edge
      list
    } else List()//get the destination node deaths

    TrackedGraphEffect(channelId, channelTime, SyncExistingRemovals(msgTime, srcId, dstId, deaths))
  }

  def syncExistingRemovals(msgTime: Long, srcId: Long, dstId: Long, dstRemovals: List[Long]): Unit = {
    if(RaphtoryActor.hasDeletions)
      getVertexOrPlaceholder(msgTime, srcId).getOutgoingEdge(dstId) match {
        case Some(edge) => edge killList dstRemovals
        case None => /*todo Should this happen*/
      }
  }

  override def deduplicate(): Unit = {
    vertices.foreach{
      case (id,vertex) => vertex.dedupe()
    }

  }

  /**
    * Analysis Functions
    * */
  override def getVertices(lens:GraphLens, time: Long, window: Long=Long.MaxValue): mutable.Map[Long, Vertex] = {
    val lenz = lens.asInstanceOf[PojoGraphLens]
    val x = vertices.collect{
      case (id,vertex) if(vertex.aliveAtWithWindow(time,window)) => (id,vertex.viewAtWithWindow(time,window,lenz))
    }
    x
  }


}
