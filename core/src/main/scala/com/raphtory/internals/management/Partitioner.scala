package com.raphtory.internals.management

import com.typesafe.config.Config

class Partitioner(config: Config) {
  val partitionServers: Int    = config.getInt("raphtory.partitions.serverCount")
  val partitionsPerServer: Int = config.getInt("raphtory.partitions.countPerServer")
  val totalPartitions: Int     = partitionServers * partitionsPerServer

  def getPartitionForId(id: Long): Int =
    (id.abs % totalPartitions).toInt
}
