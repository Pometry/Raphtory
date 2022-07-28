package com.raphtory.internals.storage.pojograph

import com.raphtory.api.input._
import com.raphtory.internals.graph.GraphAlteration._
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.graph.LensInterface
import com.raphtory.internals.storage.pojograph.entities.external.vertex.PojoExVertex
import com.raphtory.internals.storage.pojograph.entities.internal.PojoEdge
import com.raphtory.internals.storage.pojograph.entities.internal.PojoEntity
import com.raphtory.internals.storage.pojograph.entities.internal.PojoVertex
import com.raphtory.internals.storage.pojograph.entities.internal.SplitEdge
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable

private[raphtory] class PojoBasedPartition(partition: Int, conf: Config)
        extends GraphPartition(partition: Int, conf: Config) {
  private val logger: Logger        = Logger(LoggerFactory.getLogger(this.getClass))
  private val hasDeletionsPath      = "raphtory.data.containsDeletions"
  private val hasDeletions: Boolean = conf.getBoolean(hasDeletionsPath)
  logger.debug(
          s"Config indicates that the data contains 'delete' events. " +
            s"To change this modify '$hasDeletionsPath' in the application conf."
  )

  // Map of vertices contained in the partition
  private val vertices: mutable.Map[Long, PojoVertex] = mutable.Map[Long, PojoVertex]()

  def addProperties(msgTime: Long, index: Long, entity: PojoEntity, properties: Properties): Unit =
    properties.properties.foreach {
      case StringProperty(key, value)    => entity + (msgTime, index, false, key, value)
      case LongProperty(key, value)      => entity + (msgTime, index, false, key, value)
      case DoubleProperty(key, value)    => entity + (msgTime, index, false, key, value)
      case FloatProperty(key, value)     => entity + (msgTime, index, false, key, value)
      case BooleanProperty(key, value)   => entity + (msgTime, index, false, key, value)
      case IntegerProperty(key, value)   => entity + (msgTime, index, false, key, value)
      case ImmutableProperty(key, value) => entity + (msgTime, index, true, key, value)
    }

  // if the add come with some properties add all passed properties into the entity
  override def addVertex(
      msgTime: Long,
      index: Long,
      srcId: Long,
      properties: Properties,
      vertexType: Option[Type]
  ): Unit = {
    addVertexInternal(msgTime, index, srcId, properties, vertexType)
    logger.trace(s"Added vertex $srcId")
  }

  // TODO Unfolding of type is un-necessary
  def addVertexInternal(
      msgTime: Long,
      index: Long,
      srcId: Long,
      properties: Properties,
      vertexType: Option[Type]
  ): PojoVertex = { //Vertex add handler function
    val vertex: PojoVertex = vertices.get(srcId) match { //check if the vertex exists
      case Some(v) => //if it does
        v revive (msgTime, index) //add the history point
        logger.trace(s"History point added to vertex: $srcId")
        v
      case None    => //if it does not exist
        val v = new PojoVertex(msgTime, index, srcId, initialValue = true) //create a new vertex
        vertices += ((srcId, v)) //put it in the map)
        v.setType(vertexType.map(_.name))
        logger.trace(s"New vertex created $srcId")
        v
    }
    addProperties(msgTime, index, vertex, properties)
    logger.trace(s"Properties added: $properties")
    logger.trace(s"Vertex returned: ${vertex.vertexId}")
    vertex //return the vertex
  }

  def getVertexOrPlaceholder(msgTime: Long, index: Long, id: Long): PojoVertex =
    vertices.get(id) match {
      case Some(vertex) => vertex
      case None         =>
        val vertex = new PojoVertex(msgTime, index, id, initialValue = true)
        vertices put (id, vertex)
        vertex.wipe()
        vertex
    }

  def removeVertex(msgTime: Long, index: Long, srcId: Long): List[GraphUpdateEffect] = {
    val vertex = vertices.get(srcId) match {
      case Some(v) =>
        v kill (msgTime, index)
        logger.trace(s"Removed vertex $srcId")
        v
      case None    => //if the removal has arrived before the creation
        val v = new PojoVertex(msgTime, index, srcId, initialValue = false) //create a placeholder
        vertices put (srcId, v) //add it to the map
        logger.trace(s"Removed vertex $srcId")
        v
    }

    val messagesForIncoming = vertex.incomingEdges
      .map { edge =>
        edge._2 match {
          case remoteEdge: SplitEdge =>
            remoteEdge kill (msgTime, index)
            logger.trace(s"$msgTime killed in $remoteEdge")
            Some[GraphUpdateEffect](
                    InboundEdgeRemovalViaVertex(msgTime, index, remoteEdge.getSrcId, remoteEdge.getDstId)
            )
          case edge                  => //if it is a local edge -- operated by the same worker, therefore we can perform an action -- otherwise we must inform the other local worker to handle this
            edge kill (msgTime, index)
            logger.trace(s"$msgTime killed in $edge")
            None
        }
      }
      .toList
      .flatten
    val messagesForOutgoing = vertex.outgoingEdges
      .map { edge =>
        edge._2 match {
          case remoteEdge: SplitEdge =>
            remoteEdge kill (msgTime, index) //outgoing edge always operated by the same worker, therefore we can perform an action
            logger.trace(s"$msgTime killed in $remoteEdge")
            Some[GraphUpdateEffect](
                    OutboundEdgeRemovalViaVertex(msgTime, index, remoteEdge.getSrcId, remoteEdge.getDstId)
            )
          case edge                  =>
            edge kill (msgTime, index) //outgoing edge always operated by the same worker, therefore we can perform an action
            logger.trace(s"$msgTime killed in $edge")
            None
        }
      }
      .toList
      .flatten
    val messages            = messagesForIncoming ++ messagesForOutgoing
    //if (messages.size != vertex.getEdgesRequiringSync())
    //  logger.error(s"The number of Messages to sync [${messages.size}] does not match to system value [${vertex.getEdgesRequringSync()}]")
    messages
  }

  // Edge methods

  def addEdge(
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Option[Type]
  ): Option[GraphUpdateEffect] = {
    val local     = checkDst(dstId) //is the dst on this machine
    logger.trace(s"Dst is on the machine: $local")
    val srcVertex =
      addVertexInternal(msgTime, index, srcId, Properties(), None) // create or revive the source ID
    logger.trace(s"Src ID: $srcId created and revived")
    val (present, edge) = srcVertex.getOutgoingEdge(dstId) match {
      case Some(e) => //retrieve the edge if it exists
        logger.trace(s"Edge of srcID: $srcId - dstId: $dstId retrieved")
        (true, e)
      case None    => //if it does not
        val newEdge = if (local) {
          logger.trace(s"New edge created $srcId - $dstId")
          new PojoEdge(
                  msgTime,
                  index,
                  srcId,
                  dstId,
                  initialValue = true
          ) //create the new edge, local or remote
        }
        else {
          logger.trace(s"Split edge $srcId - $dstId between partitions created")
          new SplitEdge(msgTime, index, srcId, dstId, initialValue = true)
        }
        newEdge.setType(edgeType.map(_.name))
        srcVertex.addOutgoingEdge(newEdge) //add this edge to the vertex
        logger.trace(s"Added edge $newEdge to vertex $srcVertex")
        (false, newEdge)
    }

    val maybeEffect =
      if (present) {
        edge revive (msgTime, index) //if the edge was previously created we need to revive it
        logger.trace(s"Edge ${edge.getSrcId} - ${edge.getDstId} revived")
        if (local) {
          if (srcId != dstId)
            addVertexInternal(
                    msgTime,
                    index,
                    dstId,
                    Properties(),
                    None
            ) // do the same for the destination ID
          None
        }
        else
          Some(
                  SyncExistingEdgeAdd(msgTime, index, srcId, dstId, properties)
          )                          // inform the partition dealing with the destination node*/
      }
      else {
        val deaths = if (hasDeletions) {
          val list = srcVertex.deletionList
          edge killList list // add them to the edge
          logger.trace(s"Added $edge to killList: $list")
          list
        }
        else List() //we extract the removals from the src

        if (local) {
          if (srcId != dstId) {
            val dstVertex =
              addVertexInternal(
                      msgTime,
                      index,
                      dstId,
                      Properties(),
                      None
              )                                                    // do the same for the destination ID
            dstVertex addIncomingEdge edge                         // add it to the dst as would not have been seen
            logger.trace(s"added $edge to $dstVertex")
            if (hasDeletions) edge killList dstVertex.deletionList //add the dst removes into the edge
            logger.trace(s"Added ${dstVertex.deletionList} to $edge")
          }
          else {
            srcVertex addIncomingEdge edge // a self loop should be in the incoming map as well
            logger.trace(s"added $edge to $srcVertex")
          }
          None
        }
        else {
          srcVertex
            .incrementEdgesRequiringSync() //if its not fully local and is new then increment the count for edges requiring a watermark count
          Some(SyncNewEdgeAdd(msgTime, index, srcId, dstId, properties, deaths, edgeType))
        }
      }
    addProperties(msgTime, index, edge, properties)
    maybeEffect
  }

  def syncNewEdgeAdd(
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      srcRemovals: List[(Long, Long)],
      edgeType: Option[Type]
  ): GraphUpdateEffect = {
    val dstVertex =
      addVertexInternal(msgTime, index, dstId, Properties(), None) //create or revive the destination node
    logger.trace(s"created and revived destination vertex: $dstId")
    val edge   = dstVertex.getIncomingEdge(srcId) match {
      case Some(edge) =>
        logger.debug(
                s"Edge $srcId $dstId already existed in partition $partition for syncNewEdgeAdd"
        )
        edge
      case None       =>
        val e = new SplitEdge(msgTime, index, srcId, dstId, initialValue = true)
        dstVertex addIncomingEdge e
        e
    }
    logger.trace(s"added $edge to $dstVertex")
    val deaths = if (hasDeletions) {
      val list = dstVertex.deletionList
      edge killList srcRemovals //pass source node death lists to the edge
      edge killList list        // pass destination node death lists to the edge
      logger.trace(s"Passed source node and destination node death lists to respective edges")
      list
    }
    else List() //get the destination node deaths

    addProperties(msgTime, index, edge, properties)
    logger.trace(s"Added properties $properties")
    dstVertex.incrementEdgesRequiringSync()
    edge.setType(edgeType.map(_.name))
    SyncExistingRemovals(msgTime, index, srcId, dstId, deaths, fromAddition = true)
  }

  def syncExistingEdgeAdd(
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties
  ): GraphUpdateEffect = {
    val dstVertex =
      addVertexInternal(msgTime, index, dstId, Properties(), None) // revive the destination node
    logger.trace(s"Revived destination node: ${dstVertex.vertexId}")
    dstVertex.getIncomingEdge(srcId) match {
      case Some(edge) =>
        edge revive (msgTime, index) //revive the edge
        logger.trace(s"Revived edge ${edge.getSrcId} - ${edge.getDstId}")
        addProperties(msgTime, index, edge, properties)
        logger.trace(s"Added properties: $properties to edge")
      case None       =>
        logger.debug(
                s"Edge $srcId $dstId missing from partition $partition for syncExistingEdgeAdd"
        )
        val edge = new SplitEdge(msgTime, index, srcId, dstId, initialValue = true)
        addProperties(msgTime, index, edge, properties)
        dstVertex addIncomingEdge edge

    }
    EdgeSyncAck(msgTime, index, srcId, dstId, fromAddition = true)
  }

  def batchAddRemoteEdge(
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Option[Type]
  ): Unit = {
    val dstVertex =
      addVertexInternal(msgTime, index, dstId, Properties(), None) // revive the destination node
    logger.trace(s"Revived destination node: ${dstVertex.vertexId}")
    dstVertex.getIncomingEdge(srcId) match {
      case Some(edge) =>
        edge revive (msgTime, index) //revive the edge
        logger.trace(s"Revived edge ${edge.getSrcId} - ${edge.getDstId}")
        addProperties(msgTime, index, edge, properties)
        logger.trace(s"Added properties: $properties to edge")
      case None       =>
        val edge = new SplitEdge(msgTime, index, srcId, dstId, initialValue = true)
        addProperties(msgTime, index, edge, properties)
        dstVertex addIncomingEdge edge //add the edge to the associated edges of the destination node
    }
  }

  def removeEdge(msgTime: Long, index: Long, srcId: Long, dstId: Long): Option[GraphUpdateEffect] = {
    val local                 = checkDst(dstId)
    logger.trace(s"Dst ID exists: $local")
    val srcVertex: PojoVertex = getVertexOrPlaceholder(msgTime, index, srcId)

    val (present, edge) = srcVertex.getOutgoingEdge(dstId) match {
      case Some(e) =>
        (true, e)
      case None    =>
        val newEdge =
          if (local)
            new PojoEdge(msgTime, index, srcId, dstId, initialValue = false)
          else
            new SplitEdge(msgTime, index, srcId, dstId, initialValue = false)
        srcVertex.addOutgoingEdge(
                newEdge
        ) // add the edge to the associated edges of the source node
        (false, newEdge)
    }

    if (present) {
      logger.trace(s"Removing edge $edge")
      edge kill (msgTime, index)
      if (local)
        None
      else
        Some(
                SyncExistingEdgeRemoval(msgTime, index, srcId, dstId)
        ) // inform the partition dealing with the destination node
    }
    else {
      val deaths = if (hasDeletions) {
        logger.trace(s"Removing edge $edge")
        val list = srcVertex.deletionList
        edge killList list
        list
      }
      else List()

      if (local) {
        if (srcId != dstId) {
          val dstVertex =
            getVertexOrPlaceholder(msgTime, index, dstId)        // do the same for the destination ID
          logger.trace(s"Removing edge $edge of dst vertex: $dstVertex")
          dstVertex addIncomingEdge edge                         // do the same for the destination node
          if (hasDeletions) edge killList dstVertex.deletionList //add the dst removes into the edge
        }
        None
      }
      else {
        srcVertex
          .incrementEdgesRequiringSync() //if its not fully local and is new then increment the count for edges requireing a watermark count
        Some(SyncNewEdgeRemoval(msgTime, index, srcId, dstId, deaths))
      }
    }
  }

  def inboundEdgeRemovalViaVertex(msgTime: Long, index: Long, srcId: Long, dstId: Long): GraphUpdateEffect = { //for the source getting an update about deletions from a remote worker
    getVertexOrPlaceholder(msgTime, index, srcId).getOutgoingEdge(dstId) match {
      case Some(edge) => edge kill (msgTime, index)
      case None       => logger.error("Remote edge removal with no outgoing edge.")
    }
    VertexRemoveSyncAck(msgTime, index, dstId)
  }

  def syncExistingEdgeRemoval(msgTime: Long, index: Long, srcId: Long, dstId: Long): GraphUpdateEffect = {
    val dstVertex = getVertexOrPlaceholder(msgTime, index, dstId)
    dstVertex.getIncomingEdge(srcId) match {
      case Some(e) => e kill (msgTime, index)
      case None    =>
        logger.debug(
                s"Edge $srcId $dstId missing from partition $partition for syncExistingEdgeRemoval"
        )
        val edge = new SplitEdge(msgTime, index, srcId, dstId, initialValue = false)
        dstVertex addIncomingEdge edge
    }
    EdgeSyncAck(msgTime, index, srcId, dstId, fromAddition = false)
  }

  def outboundEdgeRemovalViaVertex(msgTime: Long, index: Long, srcId: Long, dstId: Long): GraphUpdateEffect = {
    getVertexOrPlaceholder(msgTime, index, dstId).getIncomingEdge(srcId) match {
      case Some(e) => e kill (msgTime, index)
      case None    => logger.error("Remote edge removal from vertex with no incoming edge.")
    }
    VertexRemoveSyncAck(msgTime, index, srcId)
  }

  def syncNewEdgeRemoval(
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      srcRemovals: List[(Long, Long)]
  ): GraphUpdateEffect = {
    val dstVertex = getVertexOrPlaceholder(msgTime, index, dstId)
    dstVertex.incrementEdgesRequiringSync()

    val edge = dstVertex.getIncomingEdge(srcId) match {
      case Some(edge) =>
        logger.debug(
                s"Edge $srcId $dstId already existed in partition $partition for syncNewEdgeRemoval"
        )
        edge
      case None       =>
        val e = new SplitEdge(msgTime, index, srcId, dstId, initialValue = false)
        dstVertex addIncomingEdge e
        e
    }

    val deaths = if (hasDeletions) {
      val list = dstVertex.deletionList
      edge killList srcRemovals //pass source node death lists to the edge
      edge killList list        // pass destination node death lists to the edge
      logger.trace("Synced New Edge Removals")
      list
    }
    else List() //get the destination node deaths

    SyncExistingRemovals(msgTime, index, srcId, dstId, deaths, fromAddition = false)
  }

  def syncExistingRemovals(
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      dstRemovals: List[(Long, Long)]
  ): Unit =
    if (hasDeletions)
      getVertexOrPlaceholder(msgTime, index, srcId).getOutgoingEdge(dstId) match {
        case Some(edge) =>
          edge killList dstRemovals
          logger.trace("Synced Existing Removals")
        case None       =>
      }

  // Analysis Functions
  override def getVertices(
      lens: LensInterface,
      start: Long,
      end: Long
  ): mutable.Map[Long, PojoExVertex] = {
    val lenz = lens.asInstanceOf[PojoGraphLens]
    val x    = vertices.collect {
      case (id, vertex) if vertex.aliveBetween(start, end) =>
        (id, vertex.viewBetween(start, end, lenz))
    }
    x
  }

}
