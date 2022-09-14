package com.raphtory.internals.storage.arrow

import com.raphtory.arrowcore.implementation.RaphtoryArrowPartition.RaphtoryArrowPartitionConfig
import com.raphtory.arrowcore.model.PropertySchema

import java.nio.file.Path

case class ArrowPartitionConfig(
    partitionId: Int,
    nPartitions: Int,
    propertySchema: PropertySchema,
    arrowDir: Path,
    nLocalEntityIdMaps: Int = Runtime.getRuntime.availableProcessors(),
    localEntityIdMapSize: Int = 64
) {

  def toRaphtoryPartitionConfig: RaphtoryArrowPartitionConfig = {

    val cfg = new RaphtoryArrowPartitionConfig

    cfg._propertySchema = propertySchema
    cfg._arrowDir = arrowDir.toString
    cfg._raphtoryPartitionId = partitionId
    cfg._nRaphtoryPartitions = nPartitions
    cfg._nLocalEntityIdMaps = nLocalEntityIdMaps
    cfg._localEntityIdMapSize = localEntityIdMapSize
    cfg._syncIDMap = true

    cfg
  }
}
