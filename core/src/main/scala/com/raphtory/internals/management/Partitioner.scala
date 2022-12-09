package com.raphtory.internals.management

import com.typesafe.config.Config

class Partitioner(config: Config) {
  val partitionServers: Int    = config.getInt("raphtory.partitions.serverCount")
  val partitionsPerServer: Int = config.getInt("raphtory.partitions.countPerServer")
  val totalPartitions: Int     = partitionServers * partitionsPerServer

  def getPartitionForId(id: Long): Int                         = (id.abs % totalPartitions).toInt
  def getPartitionsForEdge(srcId: Long, dstId: Long): Set[Int] = Set(getPartitionForId(srcId), getPartitionForId(dstId))
}

object Partitioner {
  def apply(config: Config) = new Partitioner(config)
}
