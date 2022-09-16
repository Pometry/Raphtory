package com.raphtory.internals.storage.arrow

import com.raphtory.arrowcore.implementation.LocalEntityIdStore
import com.raphtory.internals.graph.GraphPartition

class LocalEntityRepo(store: LocalEntityIdStore, totalPartitions: Int, partitionID: Int) {

  def resolve(globalId: Long): EntityId =
    store.getLocalNodeId(globalId) match {
      // not found but belongs to this partition
      case -1 if GraphPartition.checkDst(globalId, totalPartitions, partitionID) => NotFound(globalId)
      // not found and belongs to different partition
      case -1 => GlobalId(globalId)
      // already here
      case id => ExistsOnPartition(id)
    }

}
