package com.raphtory.arrowmessaging

import com.raphtory.arrowmessaging.model._
import org.apache.arrow.flight._
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.logging.log4j.LogManager
import ArrowFlightClientProvider._
import java.lang.reflect.InvocationTargetException
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import scala.util._

sealed trait ArrowFlightMessageSchemaReaderRegistry extends AutoCloseable {
  val signatureRegistry: ArrowFlightMessageSignatureRegistry

  private val factories = new ConcurrentHashMap[String, ArrowFlightMessageSchemaFactory]()

  def getSchema(endPoint: String, vectorSchemaRoot: VectorSchemaRoot): ArrowFlightMessageSchema[_, _] =
    try {
      if (!factories.containsKey(endPoint)) {
        val constructor = signatureRegistry.getSignature(endPoint).schemaFactoryClass.getDeclaredConstructor()
        val factory     = constructor.newInstance().asInstanceOf[ArrowFlightMessageSchemaFactory]
        factories.put(endPoint, factory)
      }
      val schema = factories.get(endPoint).getInstance(vectorSchemaRoot)
      schema
    }
    catch {
      case e @ (_: NoSuchMethodException | _: InstantiationException | _: IllegalAccessException |
          _: InvocationTargetException) =>
        throw new Exception("Failed to create schema", e);
    }

  def close(): Unit = {}
}

case class ArrowFlightReader[T](
    interface: String,
    port: Int,
    allocator: BufferAllocator,
    topics: Set[String],
    messageHandler: T => Unit,
    signatureRegistry: ArrowFlightMessageSignatureRegistry
) extends ArrowFlightMessageSchemaReaderRegistry {

  private val logger                = LogManager.getLogger(classOf[ArrowFlightReader[_]])
  private var totalMessagesRead     = 0L
  private var lastTotalMessagesRead = 0L
  private val flightClient          = getFlightClient(interface, port, allocator)

  logger.debug("{} is online", this)

  def readMessages(busyWaitInMilliSeconds: Long): Unit =
    while (true) {
      // try Thread.sleep(busyWaitInMilliSeconds)
      // catch {
      // case e: InterruptedException =>
      // e.printStackTrace()
      // }
      readMessages()

      if (lastTotalMessagesRead != getTotalMessagesRead)
        logger.debug("{}. Total messages read = {}", this, getTotalMessagesRead)

      lastTotalMessagesRead = getTotalMessagesRead
    }

  def readMessages(): Unit = {
    val flightInfoIter = flightClient.listFlights(Criteria.ALL)
    //if (!flightInfoIter.iterator().hasNext()) System.out.println("No data found against any endpoint!")

    // TODO Endpoints could be read in parallel
    // Iterating over endpoints
    if (flightInfoIter.iterator().hasNext)
      flightInfoIter.forEach { flightInfo =>
        val endPoint = flightInfo.getDescriptor.toString
        val header   = endPoint.substring(0, endPoint.lastIndexOf("/"))

        if (topics.contains(header)) {
          val endPointAsByteStream = flightInfo.getDescriptor.getPath.get(0).getBytes(StandardCharsets.UTF_8)

          Using(flightClient.getStream(new Ticket(endPointAsByteStream))) { flightStream =>
            var batch = 0
            logger.debug(s"$this. Reading messages for end point: ${flightInfo.getDescriptor}")

            Using(flightStream.getRoot) { vectorSchemaRootReceived =>
              vectorSchemaRootReceived.syncSchema()
              val s = endPoint.substring(endPoint.lastIndexOf("/") + 1)
              if (signatureRegistry.contains(s)) {
                val vms = getSchema(s, vectorSchemaRootReceived)
                try
                // Iterating over batches
                while (flightStream.next()) {
                  batch = batch + 1
                  // System.out.println("Reader(" + location + "). Received batch #" + batch + ", Data:")
                  var i    = 0
                  var rows = vectorSchemaRootReceived.getRowCount
                  while (i < rows) {
                    if (!vms.isMessageExistsAtRow(i))
                      logger.warn(
                              "Should not happen! location = {}, endpoint = {}, batch = {}, null at {}, row count = {}",
                              s"$interface+$port",
                              endPoint,
                              batch,
                              i,
                              rows
                      )
                    else {
                      try logger.trace(
                              "location = {}, endpoint = {}, batch = {}, vertex msg = {}, index = {}, row count = {}\n",
                              s"$interface+$port",
                              endPoint,
                              batch,
                              i,
                              vms.getMessageAtRow(i),
                              rows
                      )
                      catch {
                        case e: Exception =>
                          logger.error(
                                  "location = {}, endpoint = {}, batch = {}, index = {}, rowCount = {}, errMsg = {}",
                                  s"$interface+$port",
                                  endPoint,
                                  batch,
                                  i,
                                  rows,
                                  e.getMessage
                          )
                          e.printStackTrace()
                      }
                      // vms.getVertexMessageAtRow(i)
                      try messageHandler(vms.decodeMessage(i))
                      catch {
                        case e: Exception =>
                          logger.error(
                                  "location = {}, endpoint = {}, batch = {}, index = {}, rowCount = {}, errMsg = {}",
                                  s"$interface+$port",
                                  endPoint,
                                  batch,
                                  i,
                                  rows,
                                  e.getMessage
                          )
                      }
                    }
                    i = i + 1
                  }
                  totalMessagesRead += rows
                }
                finally if (vms != null) vms.close()

              }
            }
          } match {
            case Success(_)         =>
            case Failure(exception) => exception.printStackTrace()
          }
        }
      }
  }

  def getTotalMessagesRead: Long = totalMessagesRead

  override def close(): Unit = {
    super.close()
    logger.debug(s"$this is closed")
  }

  override def toString: String = s"ArrowFlightReader($interface,$port,${topics.toList})"
}

// For testing purposes
case class ArrowFlightMessageSchemaReaderRegistryMock(signatureRegistry: ArrowFlightMessageSignatureRegistry)
        extends ArrowFlightMessageSchemaReaderRegistry {
  override def close(): Unit = {}
}
