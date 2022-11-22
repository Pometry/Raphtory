package com.raphtory.internals.components.querymanager

import cats.syntax.all._
import cats.effect._
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.querymanager.QuerySupervisor._
import com.raphtory.internals.management.Scheduler
import com.raphtory.internals.management.telemetry.TelemetryReporter
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import cats.Traverse
import com.raphtory.protocol
import com.raphtory.protocol.PartitionService
import fs2.Stream

class QuerySupervisor[F[_]] private (
    graphID: GraphID,
    topics: TopicRepository,
    config: Config,
    partitions: Map[Int, PartitionService[F]],
    private val blockingSources: Ref[F, Map[SourceID, Deferred[F, Unit]]]
)(implicit F: Async[F]) {
  private val logger       = Logger(LoggerFactory.getLogger(this.getClass))
  private var earliestTime = -1L
  private var latestTime   = -1L

  def startBlockingIngestion(sourceID: SourceID): F[Unit] =
    for {
      defr <- Deferred[F, Unit]
      _    <- blockingSources.update(_ + (sourceID -> defr))
      _    <- F.delay(logger.info(s"Source '$sourceID' is blocking analysis for Graph '$graphID'"))
    } yield ()

  def endBlockingIngestion(sourceID: Int, _earliestTime: Long, _latestTime: Long): F[Unit] =
    for {
      _  <- F.delay {
              earliestTime = earliestTime min _earliestTime
              latestTime = latestTime max _latestTime
            }
      bs <- blockingSources.get
      _  <- bs(sourceID).complete()
      _  <- F.blocking {
              logger.info(
                      s"Source '$sourceID' is unblocking analysis for Graph '$graphID' with earliest time seen as $earliestTime and latest time seen as $latestTime"
              )
            }
    } yield ()

  def submitQuery(query: Query): F[Stream[F, protocol.QueryManagement]] =
    for {
      bs       <- blockingSources.get
      _        <- bs.values.map(x => x.get).toSeq.sequence
      response <- QueryHandlerF(earliestTime, latestTime, partitions.values.toSeq, query)
    } yield response
}

object QuerySupervisor {
  type GraphID  = String
  type SourceID = Long
  type JobID    = String

  def apply[F[_]: Async](
      graphID: GraphID,
      topics: TopicRepository,
      config: Config,
      partitions: Map[Int, PartitionService[F]]
  ): Resource[F, QuerySupervisor[F]] =
    for {
      blockingSources <- Resource.eval(Ref.of(Map[SourceID, Deferred[F, Unit]]()))
      service         <- Resource.make {
                           Async[F].delay(
                                   new QuerySupervisor(
                                           graphID,
                                           topics,
                                           config,
                                           partitions,
                                           blockingSources
                                   )
                           )
                         }(_ => Async[F].unit)
    } yield service
}
