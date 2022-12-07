package com.raphtory.internals.storage.pojograph

import com.raphtory.api.input._
import com.raphtory.internals.communication.SchemaProviderInstances._
import com.raphtory.internals.graph.GraphAlteration._
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.graph.LensInterface
import com.raphtory.internals.storage.pojograph.entities.external.vertex.PojoExVertex
import com.raphtory.internals.storage.pojograph.entities.internal.PojoEdge
import com.raphtory.internals.storage.pojograph.entities.internal.PojoEntity
import com.raphtory.internals.storage.pojograph.entities.internal.PojoVertex
import com.raphtory.internals.storage.pojograph.entities.internal.SplitEdge
import com.typesafe.config.Config

import scala.collection.mutable

private[raphtory] class PojoBasedPartition(graphID: String, partition: Int, conf: Config)
        extends GraphPartition(graphID, partition, conf) {
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
      case MutableString(key, value)   => entity + (msgTime, index, false, key, value)
      case MutableLong(key, value)     => entity + (msgTime, index, false, key, value)
      case MutableDouble(key, value)   => entity + (msgTime, index, false, key, value)
      case MutableFloat(key, value)    => entity + (msgTime, index, false, key, value)
      case MutableBoolean(key, value)  => entity + (msgTime, index, false, key, value)
      case MutableInteger(key, value)  => entity + (msgTime, index, false, key, value)
      case ImmutableString(key, value) => entity + (msgTime, index, true, key, value)
    }

  // if the add come with some properties add all passed properties into the entity
  override def addVertex(
      sourceID: Long,
      msgTime: Long,
      index: Long,
      srcId: Long,
      properties: Properties,
      vertexType: Option[Type]
  ): Unit =
    vertices.synchronized {
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
  ): PojoVertex = //Vertex add handler function
    {

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

  private def getVertexOrPlaceholder(msgTime: Long, index: Long, id: Long): PojoVertex =
    vertices.get(id) match {
      case Some(vertex) => vertex
      case None         =>
        val vertex = new PojoVertex(msgTime, index, id, initialValue = true)
        vertices put (id, vertex)
        vertex.wipe()
        vertex
    }

  // Edge methods

  override protected def addOutgoingEdge(
      sourceID: Long,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Option[Type]
  ): Unit =
    vertices.synchronized {
      val local     = isLocal(dstId) //is the dst on this machine
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

      if (present) {
        edge revive (msgTime, index) //if the edge was previously created we need to revive it
        logger.trace(s"Edge ${edge.getSrcId} - ${edge.getDstId} revived")
        if (local)
          if (srcId != dstId)
            addVertexInternal(
                    msgTime,
                    index,
                    dstId,
                    Properties(),
                    None
            )                        // do the same for the destination ID
      }
      else if (local)
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
      addProperties(msgTime, index, edge, properties)
    }

  override protected def addIncomingEdge(
      sourceID: Long,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Option[Type]
  ): Unit =
    vertices.synchronized {
      val dstVertex =
        addVertexInternal(msgTime, index, dstId, Properties(), None) //create or revive the destination node
      logger.trace(s"created and revived destination vertex: $dstId")
      val edge = dstVertex.getIncomingEdge(srcId) match {
        case Some(edge) =>
          edge revive (msgTime, index) //revive the edge
          logger.debug(s"Edge $srcId $dstId already existed in partition $partition for syncNewEdgeAdd")
          edge
        case None       =>
          val e = new SplitEdge(msgTime, index, srcId, dstId, initialValue = true)
          dstVertex addIncomingEdge e
          e
      }
      logger.trace(s"added $edge to $dstVertex")
      addProperties(msgTime, index, edge, properties)
      logger.trace(s"Added properties $properties")
      dstVertex.incrementEdgesRequiringSync()
      edge.setType(edgeType.map(_.name))
    }

  // Analysis Functions
  override def getVertices(
      lens: LensInterface,
      start: Long,
      end: Long
  ): mutable.Map[Long, PojoExVertex] =
    vertices.synchronized {
      val lenz = lens.asInstanceOf[PojoGraphLens]
      import scala.collection.parallel.CollectionConverters._

      vertices.par.collect {
        case (id, vertex) if vertex.aliveBetween(start, end) =>
          (id, vertex.viewBetween(start, end, lenz))
      }.seq
    }
}
