package com.raphtory.internals.communication.connectors

import com.raphtory.arrowmessaging._
import com.raphtory.internals.communication.CancelableListener
import com.raphtory.internals.communication.CanonicalTopic
import com.raphtory.internals.communication.Connector
import com.raphtory.internals.communication.EndPoint
import com.raphtory.internals.components.querymanager._
import com.raphtory.internals.graph.GraphAlteration._
import com.raphtory.internals.management.ZookeeperConnector
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.apache.arrow.memory._
import org.apache.curator.x.discovery.details.JsonInstanceSerializer
import org.apache.curator.x.discovery._
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.beans.BeanProperty
import scala.collection.mutable
import scala.jdk.CollectionConverters._

case class ServiceDetail(@BeanProperty var partitionId: Int) {
  def this() = this(0)
}

class ArrowFlightConnector(config: Config, signatureRegistry: ArrowFlightMessageSignatureRegistry)
        extends ZookeeperConnector
        with Connector {

  import ArrowFlightHostAddressProvider._

  private val logger               = Logger(LoggerFactory.getLogger(this.getClass))
  private val allocator            = new RootAllocator
  private val flightBatchSize      = config.getInt("raphtory.arrow.flight.batchSize")
  private val readBusyWait         = config.getInt("raphtory.arrow.flight.readBusyWait")
  private val deploymentId: String = config.getString("raphtory.deploy.id")

  // Endpoint is supposed to encapsulate a writer and knows how to send the message to the flight server
  case class ArrowFlightEndPoint[T](writer: ArrowFlightWriter) extends EndPoint[T] {

    var counter = new AtomicInteger(0)

    override def sendAsync(message: T): Unit = {

      def sendMsg[R](msg: R)(implicit endPoint: String): Unit =
        writer.synchronized {
          counter.incrementAndGet()
          writer.addToBatch(msg)
          if (counter.get() == flightBatchSize)
            writer.sendBatch()
        }

      message match {
        case msg: VertexMessage[_, _]          => sendMsg(msg)(msg.provider.endpoint)
        case msg: VertexAdd                    => sendMsg(msg)(msg.provider.endpoint)
        case msg: VertexDelete                 => sendMsg(msg)(msg.provider.endpoint)
        case msg: EdgeAdd                      => sendMsg(msg)(msg.provider.endpoint)
        case msg: EdgeDelete                   => sendMsg(msg)(msg.provider.endpoint)
        case msg: SyncNewEdgeAdd               => sendMsg(msg)(msg.provider.endpoint)
        case msg: BatchAddRemoteEdge           => sendMsg(msg)(msg.provider.endpoint)
        case msg: SyncExistingEdgeAdd          => sendMsg(msg)(msg.provider.endpoint)
        case msg: SyncExistingEdgeRemoval      => sendMsg(msg)(msg.provider.endpoint)
        case msg: SyncNewEdgeRemoval           => sendMsg(msg)(msg.provider.endpoint)
        case msg: OutboundEdgeRemovalViaVertex => sendMsg(msg)(msg.provider.endpoint)
        case msg: InboundEdgeRemovalViaVertex  => sendMsg(msg)(msg.provider.endpoint)
        case msg: SyncExistingRemovals         => sendMsg(msg)(msg.provider.endpoint)
        case msg: EdgeSyncAck                  => sendMsg(msg)(msg.provider.endpoint)
        case msg: VertexRemoveSyncAck          => sendMsg(msg)(msg.provider.endpoint)
        case _                                 => logger.error("VertexMessage or GraphAlteration expected")
      }
    }

    override def flushAsync(): CompletableFuture[Void] =
      CompletableFuture.completedFuture {
        writer.synchronized {
          if (counter.get() > 0) {
            if (counter.get() < flightBatchSize)
              writer.sendBatch()
            writer.completeSend()
            counter.set(0)
          }
          null
        }
      }

    override def close(): Unit = writer.close()

    override def closeWithMessage(message: T): Unit = {
      sendAsync(message)
      flushAsync()
      close()
    }
  }

  // Starts flight server and reader
  override def register[T](
      partitionId: Int,
      id: String,
      messageHandler: T => Unit,
      topics: Seq[CanonicalTopic[T]]
  ): CancelableListener = {

    val (server, reader) = startAndPublishAddress(partitionId, messageHandler)

    new CancelableListener {
      override def start(): Unit = Future(reader.readMessages(readBusyWait))

      override def close(): Unit = {
        server.close()
        reader.close()
      }
    }
  }

  // Starts writer and registers message schema and message handler to a given endpoint
  override def endPoint[T](srcParId: Int, topic: CanonicalTopic[T]): EndPoint[T] = {
    val addresses                               = getAddressAcrossPartitions
    val partitionId                             = topic.subTopic.split("-").last.toInt
    val ArrowFlightHostAddress(interface, port) = addresses(partitionId)

    ArrowFlightEndPoint(ArrowFlightWriter(interface, port, srcParId, allocator, signatureRegistry))
  }

  override def shutdown(): Unit = {}

  object ArrowFlightHostAddressProvider {
    case class ArrowFlightHostAddress(interface: String, port: Int)
    import cats.effect.unsafe.implicits.global

    private val partitionServers           = config.getInt("raphtory.partitions.serverCount")
    private val partitionsPerServer        = config.getInt("raphtory.partitions.countPerServer")
    private val numPartitions              = partitionServers * partitionsPerServer
    private val addresses                  = mutable.HashMap[Int, ArrowFlightHostAddress]()
    private val serviceDiscoveryAtomicPath = s"/$deploymentId/flightservers"
    private val zookeeperAddress           = config.getString("raphtory.zookeeper.address")
    private val zkClient                   = getZkClient(zookeeperAddress).allocated.unsafeRunSync()._1

    private val serviceDiscovery =
      ServiceDiscoveryBuilder
        .builder(classOf[ServiceDetail])
        .client(zkClient)
        .serializer(new JsonInstanceSerializer[ServiceDetail](classOf[ServiceDetail]))
        .basePath(serviceDiscoveryAtomicPath)
        .build()

    serviceDiscovery.start()

    // Closing service discovery as part of shutdown hook because termination of this partition
    // instance would mean flight server is down as well and pertaining entry must be removed from zk
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = serviceDiscovery.close()
    })

    def getAddressAcrossPartitions: Map[Int, ArrowFlightHostAddress] = {
      if (partitionServers > 1)
        // Poll zk for the addresses eq to the num of partitionServers and add the same to addresses map before returning the same
        while (addresses.size < numPartitions) {
          val serviceInstances = serviceDiscovery.queryForInstances("flightServer").asScala
          serviceInstances.foreach { i =>
            addresses
              .addOne(i.getPayload.partitionId, ArrowFlightHostAddress(i.getAddress, i.getPort))
          }

          if (addresses.size < numPartitions)
            TimeUnit.SECONDS.sleep(5)
        }

      addresses.toMap
    }

    def startAndPublishAddress[T](
        partitionId: Int,
        messageHandler: T => Unit
    ): (ArrowFlightServer, ArrowFlightReader[T]) = {
      // This is supposed to be called once server is started to ensure no clients are brought up before servers are started
      def publishAddress(interface: String, port: Int): Unit =
        if (partitionServers > 1) {
          val serviceInstance =
            ServiceInstance
              .builder()
              .address(interface)
              .port(port)
              .name("flightServer")
              .payload(ServiceDetail(partitionId))
              .build()

          serviceDiscovery.registerService(serviceInstance)
        }
        else
          addresses.addOne(partitionId, ArrowFlightHostAddress(interface, port))

      val server = ArrowFlightServer(allocator)
      server.waitForServerToStart()

      val interface = server.getInterface
      val port      = server.getPort

      publishAddress(interface, port)

      // A message handler encapsulates vertices for a given partition. Therefore, in order to read messages destined for vertices belonging to
      //  a given partition we need specific message handler. This is why flight readers are tied to a given message handler at declaration.
      val reader = ArrowFlightReader(interface, port, allocator, messageHandler, signatureRegistry)

      (server, reader)
    }

  }

}
