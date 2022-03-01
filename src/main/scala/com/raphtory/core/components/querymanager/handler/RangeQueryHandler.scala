package com.raphtory.core.components.querymanager.handler

import com.raphtory.core.algorithm.GraphAlgorithm
import com.raphtory.core.algorithm.OutputFormat
import com.raphtory.core.components.querymanager.QueryHandler
import com.raphtory.core.components.querymanager.QueryManagement
import com.raphtory.core.components.querymanager.QueryManager
import com.raphtory.core.config.PulsarController
import com.raphtory.core.graph.PerspectiveController
import com.typesafe.config.Config
import monix.execution.Scheduler
import org.apache.pulsar.client.api.Schema

/** @DoNotDocument */
case class RangeQueryHandler(
    queryManager: QueryManager,
    scheduler: Scheduler,
    jobID: String,
    algorithm: GraphAlgorithm,
    begin: Long,
    end: Long,
    increment: Long,
    windows: List[Long],
    outputFormat: OutputFormat,
    conf: Config,
    pulsarController: PulsarController
) extends QueryHandler(
                queryManager: QueryManager,
                scheduler,
                jobID,
                algorithm,
                outputFormat,
                conf: Config,
                pulsarController: PulsarController
        ) {

  override protected def buildPerspectiveController(latestTimestamp: Long): PerspectiveController =
    PerspectiveController.rangeQueryController(begin, end, increment, windows)
}
