package com.raphtory.internals.management.arrow

import com.raphtory.arrowmessaging.ArrowFlightReader
import com.raphtory.arrowmessaging.ArrowFlightServer
import com.raphtory.internals.communication.connectors.ArrowFlightHostAddress
import com.raphtory.internals.communication.connectors.ServiceDetail
import com.raphtory.internals.communication.repositories.ArrowFlightRepository.signatureRegistry
import com.typesafe.config.Config
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder
import org.apache.curator.x.discovery.ServiceInstance
import org.apache.curator.x.discovery.details.JsonInstanceSerializer
import com.raphtory.arrowmessaging._
import org.apache.arrow.memory.RootAllocator

import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.jdk.CollectionConverters.CollectionHasAsScala

class ZKHostAddressProvider(zkClient: CuratorFramework, config: Config) extends ArrowFlightHostAddressProvider(config) {

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
    if (numPartitions > 1)
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
    val allocator = new RootAllocator

    // This is supposed to be called once server is started to ensure no clients are brought up before servers are started
    def publishAddress(interface: String, port: Int): Unit =
      if (numPartitions > 1) {
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
