package com.raphtory.internals.components.querymanager

import cats.syntax.all._
import cats.effect._
import com.raphtory.internals.components.querymanager.QuerySupervisor._
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import com.raphtory.protocol.PartitionService
import com.raphtory.protocol.QueryUpdate
import fs2.Stream

import java.util.concurrent.ArrayBlockingQueue
import scala.annotation.tailrec

class QuerySupervisor[F[_]] protected (
    graphID: GraphID,
    config: Config,
    partitions: Map[Int, PartitionService[F]],
    private[querymanager] val earliestTime: Ref[F, Long],
    private[querymanager] val latestTime: Ref[F, Long],
    private[querymanager] val inprogressReqs: Ref[F, Set[Request]]
)(implicit F: Async[F]) {

  private[querymanager] case class QueryRequest(queryName: String, release: Deferred[F, Unit]) extends Request

  private[querymanager] case class LoadRequest(sourceID: SourceID, release: Deferred[F, Unit]) extends Request

  private val logger                    = Logger(LoggerFactory.getLogger(this.getClass))
  private[querymanager] val pendingReqs = new ArrayBlockingQueue[Request](1000)

  def startIngestion(sourceID: SourceID): F[Unit] =
    for {
      defr <- Deferred[F, Unit]
      lr   <- F.delay(LoadRequest(sourceID, defr))
      ipr  <- inprogressReqs.updateAndGet { r =>
                if (r.isEmpty || (!r.last.isInstanceOf[QueryRequest] && pendingReqs.isEmpty)) r + lr else r
              }
      _    <- if (ipr.isEmpty || (!ipr.last.isInstanceOf[QueryRequest] && pendingReqs.isEmpty))
                lr.release.complete(())
              else
                F.delay(pendingReqs.add(lr)) >>
                  F.delay(
                          logger.info(
                                  s"Source '$sourceID' queued for Graph '$graphID', waiting for queries in progress to complete"
                          )
                  ) >>
                  lr.release.get
      _    <- F.delay(logger.info(s"Source '$sourceID' is blocking analysis for Graph '$graphID'"))
    } yield ()

  def endIngestion(sourceID: Int, _earliestTime: Long, _latestTime: Long): F[Unit] = {
    @tailrec
    def queryReqsToRelease(ls: List[QueryRequest]): Seq[QueryRequest] =
      if (pendingReqs.peek().isInstanceOf[QueryRequest])
        queryReqsToRelease(pendingReqs.take().asInstanceOf[QueryRequest] :: ls)
      else ls

    for {
      _     <- earliestTime.update(_ min _earliestTime)
      _     <- latestTime.update(_ max _latestTime)
      ipr   <- inprogressReqs.updateAndGet(_.filterNot(_.asInstanceOf[LoadRequest].sourceID == sourceID))
      ls    <- if (ipr.isEmpty) F.delay(queryReqsToRelease(Nil)) else F.pure(Nil)
      // Query requests removed from pending request list should be added to the inprogress request list
      _     <- inprogressReqs.update(_ ++ ls)
      _     <- ls.map(_.release.complete(())).sequence
      eTime <- earliestTime.get
      lTime <- latestTime.get
      _     <- F.blocking {
                 logger.info(
                         s"Source '$sourceID' is unblocking analysis for Graph '$graphID' with earliest time seen as $eTime and latest time seen as $lTime"
                 )
               }
    } yield ()
  }

  private[querymanager] def processQueryRequest(query: Query): F[(Long, Long)] =
    for {
      defr <- Deferred[F, Unit]
      qr   <- F.delay(QueryRequest(query.name, defr))
      ipr  <- inprogressReqs.updateAndGet { r =>
                if (r.isEmpty || (!r.last.isInstanceOf[LoadRequest] && pendingReqs.isEmpty)) r + qr else r
              }
      _    <- if (ipr.isEmpty || (!ipr.last.isInstanceOf[LoadRequest] && pendingReqs.isEmpty))
                qr.release.complete(())
              else
                F.delay(pendingReqs.add(qr)) >>
                  F.delay(
                          logger.info(s"Blocking query request = ${query.name} for any in progress ingestion to complete")
                  ) >>
                  qr.release.get
      et   <- earliestTime.updateAndGet(_ min query.earliestSeen)
      lt   <- latestTime.updateAndGet(_ max query.latestSeen)
    } yield et -> lt

  def submitQuery(query: Query): F[Stream[F, QueryUpdate]]                     =
    for {
      t        <- processQueryRequest(query)
      response <- QueryHandler(t._1, t._2, partitions.values.toSeq, query, this)
    } yield response

  def endQuery(query: Query): F[Unit] = {
    @tailrec
    def loadReqsToRelease(ls: List[LoadRequest]): Seq[LoadRequest] =
      if (pendingReqs.peek().isInstanceOf[LoadRequest])
        loadReqsToRelease(pendingReqs.take().asInstanceOf[LoadRequest] :: ls)
      else ls

    for {
      ipr <- inprogressReqs.updateAndGet(_.filterNot(_.asInstanceOf[QueryRequest].queryName == query.name))
      ls  <- if (ipr.isEmpty) F.delay(loadReqsToRelease(Nil)) else F.pure(Nil)
      // Load requests removed from pending request list should be added to the inprogress request list
      _   <- inprogressReqs.update(_ ++ ls)
      _   <- ls.map(_.release.complete(()) >> F.delay(println(s"Unblocked load request"))).sequence
    } yield ()
  }
}

object QuerySupervisor {
  type GraphID  = String
  type SourceID = Long
  type JobID    = String

  sealed trait Request

  def apply[F[_]: Async](
      graphID: GraphID,
      config: Config,
      partitions: Map[Int, PartitionService[F]]
  ): Resource[F, QuerySupervisor[F]] =
    for {
      inprogressReqs <- Resource.eval(Ref.of(Set[Request]()))
      earliestTime   <- Resource.eval(Ref.of(Long.MaxValue))
      latestTime     <- Resource.eval(Ref.of(Long.MinValue))
      service        <- Resource.make {
                          Async[F].delay(
                                  new QuerySupervisor(
                                          graphID,
                                          config,
                                          partitions,
                                          earliestTime,
                                          latestTime,
                                          inprogressReqs
                                  )
                          )
                        }(_ => Async[F].unit)
    } yield service
}
