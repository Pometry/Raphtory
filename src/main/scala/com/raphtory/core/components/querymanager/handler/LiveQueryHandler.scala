package com.raphtory.core.components.querymanager.handler

import com.raphtory.core.algorithm.GraphAlgorithm
import com.raphtory.core.algorithm.OutputFormat
import com.raphtory.core.components.querymanager.QueryHandler
import com.raphtory.core.components.querymanager.QueryManagement
import com.raphtory.core.components.querymanager.QueryManager
import com.raphtory.core.config.PulsarController
import com.raphtory.core.graph.PerspectiveController
import com.raphtory.core.time.Interval
import com.typesafe.config.Config
import monix.execution.Scheduler
import org.apache.pulsar.client.api.Schema

/** @DoNotDocument */
case class LiveQueryHandler(
    queryManager: QueryManager,
    scheduler: Scheduler,
    jobID: String,
    algorithm: GraphAlgorithm,
    increment: Long,
    windows: List[Interval],
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
    PerspectiveController.liveQueryController(latestTimestamp, increment, windows)
}
