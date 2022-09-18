package com.raphtory.arrowmessaging

import com.raphtory.arrowmessaging.model._
import org.apache.arrow.flight.FlightClient.ClientStreamListener
import org.apache.arrow.flight._
import org.apache.arrow.memory._
import org.apache.logging.log4j._
import java.lang.reflect.InvocationTargetException
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable
import ArrowFlightClientProvider._

sealed trait ArrowFlightMessageSchemaWriterRegistry extends AutoCloseable {
  val allocator: BufferAllocator
  val signatureRegistry: ArrowFlightMessageSignatureRegistry

  private val schemaRegistry = new ConcurrentHashMap[String, ArrowFlightMessageSchema[_, _]]()

  def getSchema(endPoint: String): ArrowFlightMessageSchema[_, _] = {
    if (!schemaRegistry.containsKey(endPoint))
      try {
        val constructor = signatureRegistry.getSignature(endPoint).schemaFactoryClass.getDeclaredConstructor()
        val factory     = constructor.newInstance().asInstanceOf[ArrowFlightMessageSchemaFactory]
        val schema      = factory.getInstance(allocator)
        schemaRegistry.put(endPoint, schema)
      }
      catch {
        case e @ (_: NoSuchMethodException | _: InstantiationException | _: IllegalAccessException |
            _: InvocationTargetException) =>
          throw new Exception("Failed to create instance of vertex message signature", e);
      }
    schemaRegistry.get(endPoint)
  }

  def removeSchema(endPoint: String): Unit =
    schemaRegistry.remove(endPoint)

  def close(): Unit = {
    schemaRegistry.values().forEach(_.close())
    schemaRegistry.clear()
  }
}

case class ArrowFlightWriter(
    interface: String,
    port: Int,
    topic: String,
    allocator: BufferAllocator,
    signatureRegistry: ArrowFlightMessageSignatureRegistry
) extends ArrowFlightMessageSchemaWriterRegistry {

  private val logger       = LogManager.getLogger(classOf[ArrowFlightWriter])
  private val flightClient = getFlightClient(interface, port, allocator)
  private val listeners    = mutable.HashMap[String, ClientStreamListener]()

  logger.debug("{} is online", this)

  @throws(classOf[Exception])
  def addToBatch[T](message: T)(implicit endPoint: String): Unit = {
    if (!signatureRegistry.contains(endPoint))
      throw new Exception("No schema register against endpoint = " + getAbsoluteEndpoint(topic, endPoint))

    val schema = getSchema(endPoint)

    if (!listeners.contains(endPoint)) {
      schema.allocateNew()
      listeners.put(
              endPoint,
              flightClient.startPut(
                      FlightDescriptor.path(getAbsoluteEndpoint(topic, endPoint)),
                      schema.vectorSchemaRoot,
                      new AsyncPutListener()
              )
      )
    }

    schema.addMessages(schema.encodeMessage(message))
  }

  def sendBatch(): Unit = {
    val activeEndpoints = listeners.keys
    activeEndpoints.foreach(endpoint => getSchema(endpoint).completeAddMessages())
    listeners.values.foreach(_.putNext())
    activeEndpoints.foreach(endpoint => getSchema(endpoint).clear())
  }

  def completeSend(): Unit =
    try {
      listeners.values.foreach { listener =>
        listener.completed()
        listener.getResult()
      }
      val activeEndpoints = listeners.keys
      activeEndpoints.foreach(endpoint => getSchema(endpoint).clear())
      listeners.clear()
      logger.debug(this + ": Completed Send")
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    }

  override def close(): Unit = {
    super.close()
    logger.debug(s"$this is closed")
  }

  private def getAbsoluteEndpoint(topic: String, endPoint: String): String = topic + "/" + endPoint

  override def toString: String = s"ArrowFlightWriter($interface,$port,$topic)"
}

// For testing purposes
case class ArrowFlightMessageSchemaWriterRegistryMock(
    allocator: BufferAllocator,
    signatureRegistry: ArrowFlightMessageSignatureRegistry
) extends ArrowFlightMessageSchemaWriterRegistry
