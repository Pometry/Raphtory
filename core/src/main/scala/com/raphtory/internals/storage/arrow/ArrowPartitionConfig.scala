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
    vertexPartitionSize: Option[Int] = None,
    edgePartitionSize: Option[Int] = None
) {

  def toRaphtoryPartitionConfig: RaphtoryArrowPartitionConfig = {

    val cfg = new RaphtoryArrowPartitionConfig

    cfg._propertySchema = propertySchema
    cfg._arrowDir = arrowDir.toString
    cfg._raphtoryPartitionId = partitionId
    cfg._nRaphtoryPartitions = nPartitions
    cfg._nLocalEntityIdMaps = nLocalEntityIdMaps
    cfg._localEntityIdMapSize = nLocalEntityIdMaps
    vertexPartitionSize.foldLeft(cfg) { (cfg, n) => cfg._vertexPartitionSize = n; cfg }
    edgePartitionSize.foldLeft(cfg) { (cfg, n) => cfg._edgePartitionSize = n; cfg }
    cfg._syncIDMap = true

    cfg
  }
}

object ArrowPartitionConfig {

  def apply(
      config: Config,
      partitionId: Int,
      propertySchema: PropertySchema,
      arrowDir: Path,
      vertexPartitionSize: Option[Int] = None,
      edgePartitionSize: Option[Int] = None
  ): ArrowPartitionConfig = {
    val partitionServers: Int    = config.getInt("raphtory.partitions.serverCount")
    val partitionsPerServer: Int = config.getInt("raphtory.partitions.countPerServer")
    val totalPartitions: Int     = partitionServers * partitionsPerServer

    new ArrowPartitionConfig(
            partitionId = partitionId,
            nPartitions = totalPartitions,
            propertySchema = propertySchema,
            arrowDir = arrowDir,
            vertexPartitionSize = vertexPartitionSize,
            edgePartitionSize = edgePartitionSize
    )
  }
}
