package com.raphtory.core.components.querymanager.handler

import com.raphtory.core.components.querymanager.{PerspectiveController, QueryHandler}
import com.raphtory.core.model.algorithm.GraphAlgorithm

case class LiveQueryHandler(jobID:String, algorithm:GraphAlgorithm, increment:Long, windows: List[Long]) extends QueryHandler(jobID,algorithm) {
  override def receive: Receive = ???

  override protected def buildSubTaskController(latestTimestamp: Long): PerspectiveController =
    PerspectiveController.liveQueryController(latestTimestamp,increment,windows)
}
