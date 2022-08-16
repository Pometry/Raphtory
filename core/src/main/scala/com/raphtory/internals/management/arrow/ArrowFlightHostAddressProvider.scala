package com.raphtory.internals.management.arrow

import com.raphtory.arrowmessaging.ArrowFlightReader
import com.raphtory.arrowmessaging.ArrowFlightServer
import com.raphtory.internals.communication.connectors.ArrowFlightHostAddress
import com.typesafe.config.Config
import scala.collection.concurrent.TrieMap

abstract class ArrowFlightHostAddressProvider(config: Config) {
  protected val partitionServers: Int    = config.getInt("raphtory.partitions.serverCount")
  protected val partitionsPerServer: Int = config.getInt("raphtory.partitions.countPerServer")
  protected val numPartitions: Int       = partitionServers * partitionsPerServer

  protected val addresses: TrieMap[Int, ArrowFlightHostAddress] = TrieMap[Int, ArrowFlightHostAddress]()
  protected val deploymentID: String                            = config.getString("raphtory.deploy.id")
  protected val serviceDiscoveryAtomicPath: String              = s"/$deploymentID/flightservers"

  def getAddressAcrossPartitions: Map[Int, ArrowFlightHostAddress]

  def startAndPublishAddress[T](
      partitionId: Int,
      messageHandler: T => Unit
  ): (ArrowFlightServer, ArrowFlightReader[T])
}
