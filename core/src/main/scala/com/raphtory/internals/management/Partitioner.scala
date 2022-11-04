package com.raphtory.internals.management

import com.raphtory.internals.management.GraphConfig.ConfigBuilder
import com.typesafe.config.Config

class Partitioner {
  val config: Config = ConfigBuilder.getDefaultConfig
  val partitionServers: Int    = config.getInt("raphtory.partitions.serverCount")
  val partitionsPerServer: Int = config.getInt("raphtory.partitions.countPerServer")
  val totalPartitions: Int     = partitionServers * partitionsPerServer

  def getPartitionForId(id: Long): Int =
    (id.abs % totalPartitions).toInt
}

object Partitioner {
  def apply() = new Partitioner()
}
