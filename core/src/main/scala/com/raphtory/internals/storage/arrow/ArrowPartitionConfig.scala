package com.raphtory.internals.storage.arrow

import com.raphtory.arrowcore.implementation.RaphtoryArrowPartition.RaphtoryArrowPartitionConfig
import com.raphtory.arrowcore.model.PropertySchema
import com.typesafe.config.Config

import java.nio.file.Path

class ArrowPartitionConfig(
    partitionId: Int,
    nPartitions: Int,
    propertySchema: PropertySchema,
    arrowDir: Path,
    nLocalEntityIdMaps: Int = Runtime.getRuntime.availableProcessors(),
) {

  def toRaphtoryPartitionConfig: RaphtoryArrowPartitionConfig = {

    val cfg = new RaphtoryArrowPartitionConfig

    cfg._propertySchema = propertySchema
    cfg._arrowDir = arrowDir.toString
    cfg._raphtoryPartitionId = partitionId
    cfg._nRaphtoryPartitions = nPartitions
    cfg._nLocalEntityIdMaps = nLocalEntityIdMaps
//    cfg._localEntityIdMapSize = localEntityIdMapSize
//    cfg._vertexPartitionSize = 32
//    cfg._edgePartitionSize = 256
    cfg._syncIDMap = true

    cfg
  }
}

object ArrowPartitionConfig {

  def apply(config: Config, partitionId: Int, propertySchema: PropertySchema, arrowDir: Path): ArrowPartitionConfig = {
    val partitionServers: Int    = config.getInt("raphtory.partitions.serverCount")
    val partitionsPerServer: Int = config.getInt("raphtory.partitions.countPerServer")
    val totalPartitions: Int     = partitionServers * partitionsPerServer

    new ArrowPartitionConfig(
            partitionId = partitionId,
            nPartitions = totalPartitions,
            propertySchema = propertySchema,
            arrowDir = arrowDir
    )
  }
}
