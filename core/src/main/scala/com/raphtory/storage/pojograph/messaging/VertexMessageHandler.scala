package com.raphtory.storage.pojograph.messaging

import java.util.concurrent.atomic.AtomicInteger
import com.raphtory.components.querymanager.GenericVertexMessage
import com.raphtory.components.querymanager.QueryManagement
import com.raphtory.components.querymanager.VertexMessage
import com.raphtory.components.querymanager.VertexMessageBatch
import com.raphtory.config.EndPoint
import com.raphtory.serialisers.PulsarKryoSerialiser
import com.raphtory.storage.pojograph.PojoGraphLens
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.apache.pulsar.client.api.Producer
import org.slf4j.LoggerFactory

import java.util.concurrent.CompletableFuture
import scala.collection.mutable
import scala.concurrent.Future

/** @DoNotDocument */
class VertexMessageHandler(
    config: Config,
    producers: Map[Int, EndPoint[QueryManagement]],
    pojoGraphLens: PojoGraphLens,
    sentMessages: AtomicInteger,
    receivedMessages: AtomicInteger
) {
  private val kryo: PulsarKryoSerialiser = PulsarKryoSerialiser()

  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  val msgBatchPath: String  = "raphtory.partitions.batchMessages"
  val messageBatch: Boolean = config.getBoolean(msgBatchPath)
  val maxBatchSize: Int     = config.getInt("raphtory.partitions.maxMessageBatchSize")

  if (messageBatch)
    logger.debug(
            s"Message batching is set to on. To change this modify '$msgBatchPath' in the application conf."
    )

  val totalPartitions: Int = config.getInt("raphtory.partitions.countPerServer") * config.getInt(
          "raphtory.partitions.serverCount"
  )
  logger.debug(s"Setting total partitions to '$totalPartitions'.")

  private val messageCache =
    mutable.Map[EndPoint[QueryManagement], mutable.ArrayBuffer[GenericVertexMessage]]()
  refreshBuffers()

  def sendMessage(message: GenericVertexMessage): Unit = {
    sentMessages.incrementAndGet()
    val destinationPartition = (message.vertexId.abs % totalPartitions).toInt
    if (destinationPartition == pojoGraphLens.partitionID) { //sending to this partition
      pojoGraphLens.receiveMessage(message)
      receivedMessages.incrementAndGet()
    }
    else { //sending to a remote partition
      val producer = producers(destinationPartition)
      if (messageBatch) {
        val cache = messageCache(producer)
        cache += message
        if (cache.size > maxBatchSize)
          sendCached(producer)
      }
      else
        producer sendAsync message
    }

  }

  def sendCached(readerJobWorker: EndPoint[QueryManagement]): Unit = {
    readerJobWorker sendAsync VertexMessageBatch(messageCache(readerJobWorker).toArray)
    messageCache.put(readerJobWorker, mutable.ArrayBuffer[GenericVertexMessage]())
  }

  def flushMessages(): CompletableFuture[Void] = {
    logger.debug("Flushing messages in vertex handler.")

    if (messageBatch)
      messageCache.keys.foreach(producer => sendCached(producer))
    val futures = producers.values.map(_.flushAsync())
    CompletableFuture.allOf(futures.toSeq: _*)
  }

  private def refreshBuffers(): Unit = {
    logger.debug("Refreshing messageCache buffers for all Producers.")

    producers.foreach {
      case (key, producer) =>
        messageCache.put(producer, mutable.ArrayBuffer[GenericVertexMessage]())
    }
  }

}

object VertexMessageHandler {

  def apply(
      config: Config,
      producers: Map[Int, EndPoint[QueryManagement]],
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
