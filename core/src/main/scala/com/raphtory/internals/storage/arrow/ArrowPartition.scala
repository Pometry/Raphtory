package com.raphtory.internals.storage.arrow

import com.raphtory.arrowcore.implementation.RaphtoryArrowPartition

class ArrowPartition(par: RaphtoryArrowPartition) {}

object ArrowPartition {

  def apply(cfg: ArrowPartitionConfig): ArrowPartition =
    new ArrowPartition(new RaphtoryArrowPartition(cfg.toRaphtoryPartitionConfig))
}
