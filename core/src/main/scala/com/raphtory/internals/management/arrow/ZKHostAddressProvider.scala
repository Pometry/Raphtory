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
import com.raphtory.internals.communication.CanonicalTopic
import org.apache.arrow.memory.RootAllocator

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.jdk.CollectionConverters.CollectionHasAsScala

class ZKHostAddressProvider(
    zkClient: CuratorFramework,
    config: Config,
    server: ArrowFlightServer,
    allocator: RootAllocator
) extends ArrowFlightHostAddressProvider(config) {

  private val interface = server.getInterface
  private val port      = server.getPort

  private val serviceDiscovery =
    ServiceDiscoveryBuilder
      .builder(classOf[ServiceDetail])
      .client(zkClient)
      .serializer(new JsonInstanceSerializer[ServiceDetail](classOf[ServiceDetail]))
      //.basePath(serviceDiscoveryAtomicPath)
      .basePath("")
      .build()

  serviceDiscovery.start()

  // Closing service discovery as part of shutdown hook because termination of this partition
  // instance would mean flight server is down as well and pertaining entry must be removed from zk
  Runtime.getRuntime.addShutdownHook(new Thread() {

    override def run(): Unit =
      server.close()
  })

  def getAddressAcrossPartitions(topic: String): Map[String, ArrowFlightHostAddress] = {
    // Poll zk for the addresses eq to the num of partitionServers and add the same to addresses map before returning the same
    while (!addresses.contains(topic)) {
      val serviceInstances = serviceDiscovery.queryForInstances(topic).asScala
      serviceInstances.foreach { i =>
        addresses
          .addOne(topic, ArrowFlightHostAddress(i.getAddress, i.getPort))
      }
      if (addresses.size < numPartitions)
        TimeUnit.SECONDS.sleep(1)
    }

    addresses.toMap
  }

  def startAndPublishAddress[T](
      topics: Seq[CanonicalTopic[T]],
      messageHandler: T => Unit
  ): ArrowFlightReader[T] = {
    val stringTopics = topics.map(_.toString).toSet
    // This is supposed to be called once server is started to ensure no clients are brought up before servers are started
    def publishAddress(topic: String, interface: String, port: Int): Unit = {
      val serviceInstance =
        ServiceInstance
          .builder()
          .address(interface)
          .port(port)
          .name(topic)
          .payload(ServiceDetail(topic))
          .build()
      serviceDiscovery.registerService(serviceInstance)
    }

    stringTopics.foreach { topic =>
      publishAddress(topic, server.getInterface, server.getPort)
      addresses.addOne((topic, ArrowFlightHostAddress(interface, port)))
    }

    // A message handler encapsulates vertices for a given partition. Therefore, in order to read messages destined for vertices belonging to
    //  a given partition we need specific message handler. This is why flight readers are tied to a given message handler at declaration.
    ArrowFlightReader(interface, port, allocator, stringTopics, messageHandler, signatureRegistry)

  }

}
