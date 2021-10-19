package com.raphtory.core.components.querymanager.handler

import com.raphtory.core.components.querymanager.{PerspectiveController, QueryHandler}
import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphFunction, TableFunction}

case class RangeQueryHandler(jobID: String,graphFuncs:List[GraphFunction],tableFuncs:List[TableFunction], start: Long, end: Long, increment: Long, windows: List[Long]) extends QueryHandler(jobID,graphFuncs,tableFuncs) {
  override protected def buildPerspectiveController(latestTimestamp: Long): PerspectiveController =
    PerspectiveController.rangeQueryController(start,end,increment,windows)
}
