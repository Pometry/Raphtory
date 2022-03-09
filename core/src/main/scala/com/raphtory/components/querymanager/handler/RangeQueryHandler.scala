package com.raphtory.components.querymanager.handler

import com.raphtory.algorithms.api.GraphAlgorithm
import com.raphtory.algorithms.api.OutputFormat
import com.raphtory.components.querymanager.QueryHandler
import com.raphtory.components.querymanager.QueryManagement
import com.raphtory.components.querymanager.QueryManager
import com.raphtory.config.PulsarController
import com.raphtory.graph.PerspectiveController
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
