package com.raphtory.core.components.graphbuilder

import com.raphtory.core.graph._
import com.typesafe.scalalogging.Logger
import net.openhft.hashing.LongHashFunction
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait GraphBuilder[T] {
  val logger: Logger                            = Logger(LoggerFactory.getLogger(this.getClass))
  private var updates: ArrayBuffer[GraphUpdate] = ArrayBuffer()

  def parseTuple(tuple: T): Unit

  def assignID(uniqueChars: String): Long = LongHashFunction.xx3().hashChars(uniqueChars)

  private[graphbuilder] def getUpdates(tuple: T): List[GraphUpdate] = {
    try {
      logger.trace(s"Parsing tuple: $tuple")
      parseTuple(tuple)
    }
    catch {
      case e: Exception     =>
        logger.warn(s"Failed to parse tuple.", e.getMessage)
      case other: Throwable =>
        logger.error("Failed to get updates.", other.getMessage)
    }

    val toReturn = updates
    updates = ArrayBuffer()

    toReturn.toList
  }

  protected def addVertex(updateTime: Long, srcId: Long): Unit =
    updates += VertexAdd(updateTime, srcId, Properties(), None)

  protected def addVertex(updateTime: Long, srcId: Long, properties: Properties): Unit =
    updates += VertexAdd(updateTime, srcId, properties, None)

  protected def addVertex(updateTime: Long, srcId: Long, vertexType: Type): Unit =
    updates += VertexAdd(updateTime, srcId, Properties(), Some(vertexType))

  protected def addVertex(
      updateTime: Long,
      srcId: Long,
      properties: Properties,
      vertexType: Type
  ): Unit =
    updates += VertexAdd(updateTime, srcId, properties, Some(vertexType))

  protected def deleteVertex(updateTime: Long, srcId: Long): Unit =
    updates += VertexDelete(updateTime, srcId)

  protected def addEdge(updateTime: Long, srcId: Long, dstId: Long): Unit =
    updates += EdgeAdd(updateTime, srcId, dstId, Properties(), None)

  protected def addEdge(updateTime: Long, srcId: Long, dstId: Long, properties: Properties): Unit =
    updates += EdgeAdd(updateTime, srcId, dstId, properties, None)

  protected def addEdge(updateTime: Long, srcId: Long, dstId: Long, edgeType: Type): Unit =
    updates += EdgeAdd(updateTime, srcId, dstId, Properties(), Some(edgeType))

  protected def addEdge(
      updateTime: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Type
  ): Unit =
    updates += EdgeAdd(updateTime, srcId, dstId, properties, Some(edgeType))

  protected def deleteEdge(updateTime: Long, srcId: Long, dstId: Long): Unit =
    updates += EdgeDelete(updateTime, srcId, dstId)

}

object GraphBuilder {

  def assignID(uniqueChars: String): Long =
    LongHashFunction.xx3().hashChars(uniqueChars)
}
