package com.raphtory.internals.management.arrow

import com.raphtory.arrowmessaging.ArrowFlightReader
import com.raphtory.arrowmessaging.ArrowFlightServer
import com.raphtory.internals.communication.connectors.ArrowFlightHostAddress
import com.raphtory.internals.communication.repositories.ArrowFlightRepository.signatureRegistry
import com.typesafe.config.Config
import org.apache.arrow.memory.RootAllocator

class LocalHostAddressProvider(config: Config) extends ArrowFlightHostAddressProvider(config) {
  private val allocator = new RootAllocator
  private val server    = ArrowFlightServer(allocator)
  server.waitForServerToStart()
  private val interface = server.getInterface
  private val port      = server.getPort

  override def getAddressAcrossPartitions: Map[Int, ArrowFlightHostAddress] = addresses.toMap

  override def startAndPublishAddress[T](
      partitionId: Int,
      messageHandler: T => Unit
  ): (ArrowFlightServer, ArrowFlightReader[T]) = {
    addresses.addOne(partitionId, ArrowFlightHostAddress(interface, port))
    val reader = ArrowFlightReader(interface, port, allocator, messageHandler, signatureRegistry)
    (server, reader)
  }
}
