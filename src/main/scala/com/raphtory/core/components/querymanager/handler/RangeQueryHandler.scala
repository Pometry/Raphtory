package com.raphtory.core.components.querymanager.handler

import com.raphtory.core.model.algorithm.GraphAlgorithm

case class RangeQueryHandler(jobId: String,algorithm:GraphAlgorithm, start: Long, end: Long, increment: Long, windows: List[Long]) extends QueryHandler {
  override def receive: Receive = ???
}
