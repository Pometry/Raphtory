package com.raphtory.core.components.querymanager.handler

import com.raphtory.core.components.querymanager.{PerspectiveController, QueryHandler}
import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphFunction, TableFunction}

case class LiveQueryHandler(jobID:String, graphFuncs:List[GraphFunction],tableFuncs:List[TableFunction], increment:Long, windows: List[Long]) extends QueryHandler(jobID,graphFuncs,tableFuncs) {
  override protected def buildPerspectiveController(latestTimestamp: Long): PerspectiveController =
    PerspectiveController.liveQueryController(latestTimestamp,increment,windows)
}
