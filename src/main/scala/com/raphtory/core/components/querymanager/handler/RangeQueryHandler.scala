package com.raphtory.core.components.querymanager.handler

import com.raphtory.core.components.querymanager.{PerspectiveController, QueryHandler}
import com.raphtory.core.model.algorithm.GraphAlgorithm

case class RangeQueryHandler(jobID: String,algorithm:GraphAlgorithm, start: Long, end: Long, increment: Long, windows: List[Long]) extends QueryHandler(jobID,algorithm) {
  override protected def buildSubTaskController(latestTimestamp: Long): PerspectiveController =
    PerspectiveController.rangeQueryController(start,end,increment,windows)
}
