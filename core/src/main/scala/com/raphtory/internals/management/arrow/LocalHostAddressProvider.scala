package com.raphtory.internals.management.arrow

import com.raphtory.arrowmessaging.ArrowFlightReader
import com.raphtory.arrowmessaging.ArrowFlightServer
import com.raphtory.internals.communication.connectors.ArrowFlightHostAddress
import com.raphtory.internals.communication.repositories.ArrowFlightRepository.signatureRegistry
import com.typesafe.config.Config
import org.apache.arrow.memory.RootAllocator

class LocalHostAddressProvider(config: Config) extends ArrowFlightHostAddressProvider(config) {
  override def getAddressAcrossPartitions: Map[Int, ArrowFlightHostAddress] = addresses.toMap

  override def startAndPublishAddress[T](
      partitionId: Int,
      messageHandler: T => Unit
  ): (ArrowFlightServer, ArrowFlightReader[T]) = {
    val allocator = new RootAllocator
    val server    = ArrowFlightServer(allocator)
    server.waitForServerToStart()
    val interface = server.getInterface
    val port      = server.getPort
    addresses.addOne(partitionId, ArrowFlightHostAddress(interface, port))
    val reader    = ArrowFlightReader(interface, port, allocator, messageHandler, signatureRegistry)
    (server, reader)
  }
}
