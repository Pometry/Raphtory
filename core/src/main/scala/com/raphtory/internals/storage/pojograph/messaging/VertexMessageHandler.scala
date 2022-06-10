package com.raphtory.internals.storage.pojograph.messaging

import com.raphtory.internals.communication.EndPoint
import com.raphtory.internals.components.querymanager.GenericVertexMessage
import com.raphtory.internals.components.querymanager.QueryManagement
import com.raphtory.internals.components.querymanager.VertexMessageBatch
import com.raphtory.internals.storage.pojograph.PojoGraphLens
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

private[raphtory] class VertexMessageHandler(
    config: Config,
    endPoints: Option[Map[Int, EndPoint[QueryManagement]]],
    pojoGraphLens: PojoGraphLens,
    sentMessages: AtomicInteger,
    receivedMessages: AtomicInteger
) {

  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private val msgBatchPath: String  = "raphtory.partitions.batchMessages"
  private val messageBatch: Boolean = config.getBoolean(msgBatchPath)
  private val maxBatchSize: Int     = config.getInt("raphtory.partitions.maxMessageBatchSize")

  if (messageBatch)
    logger.debug(
            s"Message batching is set to on. To change this modify '$msgBatchPath' in the application conf."
    )

  private val totalPartitions: Int =
    config.getInt("raphtory.partitions.countPerServer") *
      config.getInt("raphtory.partitions.serverCount")

  logger.debug(s"Setting total partitions to '$totalPartitions'.")

  private val messageCache =
    mutable.Map[EndPoint[QueryManagement], mutable.ArrayBuffer[GenericVertexMessage[_]]]()
  refreshBuffers()

  def sendMessage(message: GenericVertexMessage[_]): Unit = {
    val vId                  = message.vertexId match {
      case (v: Long, _) => v
      case v: Long      => v
    }
    sentMessages.incrementAndGet()
    val destinationPartition = (vId.abs % totalPartitions).toInt
    if (destinationPartition == pojoGraphLens.partitionID) { //sending to this partition
      pojoGraphLens.receiveMessage(message)
      receivedMessages.incrementAndGet()
    }
    else { //sending to a remote partition
      val producer = endPoints.get(destinationPartition)
      if (messageBatch) {
        val cache = messageCache(producer)
        cache.synchronized {
          cache += message
          if (cache.size > maxBatchSize)
            sendCached(producer)
        }
      }
      else
        producer sendAsync message
    }

  }

  def sendCached(readerJobWorker: EndPoint[QueryManagement]): Unit = {
    readerJobWorker sendAsync VertexMessageBatch(messageCache(readerJobWorker).toArray)
    messageCache(readerJobWorker).clear() // synchronisation breaks if we create a new object here
  }

  def flushMessages(): CompletableFuture[Void] = {
    logger.debug("Flushing messages in vertex handler.")

    if (messageBatch)
      messageCache.keys.foreach(producer => sendCached(producer))
    endPoints match {
      case Some(producers) =>
        val futures = producers.values.map(_.flushAsync())
        CompletableFuture.allOf(futures.toSeq: _*)
      case None            => CompletableFuture.completedFuture(null)
    }
  }

  private def refreshBuffers(): Unit = {
    logger.debug("Refreshing messageCache buffers for all Producers.")

    endPoints.foreach(_.foreach {
      case (key, producer) =>
        messageCache.put(producer, mutable.ArrayBuffer[GenericVertexMessage[_]]())
    })
  }

}

private[raphtory] object VertexMessageHandler {

  def apply(
      config: Config,
      producers: Option[Map[Int, EndPoint[QueryManagement]]],
      pojoGraphLens: PojoGraphLens,
      sentMessages: AtomicInteger,
      receivedMessages: AtomicInteger
  ) =
    new VertexMessageHandler(
            config: Config,
            producers,
            pojoGraphLens,
            sentMessages,
            receivedMessages
    )
}
