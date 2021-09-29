package com.raphtory.core.components.querymanager

import com.raphtory.core.components.RaphtoryActor
import com.raphtory.core.model.algorithm.GraphAlgorithm

abstract class QueryHandler(jobID:String,algorithm:GraphAlgorithm) extends RaphtoryActor{
  protected def buildSubTaskController(latestTimestamp: Long): PerspectiveController
}
