package com.raphtory.core.components.querymanager.handler

import com.raphtory.core.components.querymanager.QueryHandler
import com.raphtory.core.model.algorithm.GraphAlgorithm

case class PointQueryHandler(jobId: String,algorithm:GraphAlgorithm,time:Long,windows:List[Long]) extends QueryHandler {
  override def receive: Receive = ???
}
