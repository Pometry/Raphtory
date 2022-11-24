package com.raphtory.internals.components.querymanager

import cats.Parallel
import cats.effect.Async
import cats.effect.Deferred
import cats.syntax.all._
import com.raphtory.api.analysis.graphstate.GraphStateImplementation
import com.raphtory.api.analysis.graphview.ExplodeSelectWithGraph
import com.raphtory.api.analysis.graphview.GlobalGraphFunction
import com.raphtory.api.analysis.graphview.GlobalSelect
import com.raphtory.api.analysis.graphview.Iterate
import com.raphtory.api.analysis.graphview.IterateWithGraph
import com.raphtory.api.analysis.graphview.SelectWithGraph
import com.raphtory.api.analysis.graphview.SetGlobalState
import com.raphtory.api.analysis.graphview.StepWithGraph
import com.raphtory.api.analysis.graphview.TabularisingGraphFunction
import com.raphtory.api.analysis.table.Row
import com.raphtory.internals.components.output.PerspectiveResult
import com.raphtory.internals.components.output.TableOutputSink
import com.raphtory.internals.graph.LensInterface
import com.raphtory.internals.graph.Perspective
import com.raphtory.internals.graph.PerspectiveController
import com.raphtory.internals.serialisers.KryoSerialiser
import com.raphtory.protocol
import com.raphtory.protocol.GraphId
import com.raphtory.protocol.NodeCount
import com.raphtory.protocol.Operation
import com.raphtory.protocol.OperationAndState
import com.raphtory.protocol.PartitionResult
import com.raphtory.protocol.PartitionService
import com.raphtory.protocol.PerspectiveCommand
import com.raphtory.protocol.QueryId
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.util.Success

class QueryHandlerF[F[_]](
    graphId: String,
    query: Query,
    partitions: Seq[PartitionService[F]]
)(implicit F: Async[F]) {

  type GlobalTabularisingFunction = TabularisingGraphFunction with GlobalGraphFunction

  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))
  private val jobId          = query.name
  private val kryo           = KryoSerialiser()

  def processQuery(firstTimestamp: Long, lastTimestamp: Long): F[fs2.Stream[F, QueryManagement]] = {
    val queryProcessing = for {
      _          <- partitionFunction(_.establishExecutor(protocol.Query(TryQuery(Success(query)))))
      controller <- Async[F].delay(PerspectiveController(firstTimestamp, lastTimestamp, query))
      responses  <- F.pure(processAllPerspectives(controller) ++ fs2.Stream.eval(endQuery))
    } yield responses

    queryProcessing.handleErrorWith(e => Async[F].pure(fs2.Stream(JobFailed(e))))
  }

  private def endQuery = partitionFunction(_.endQuery(QueryId(graphId, jobId))).as(JobDone)

  private def partitionFunction[T](function: PartitionService[F] => F[T]) =
    Async[F].parSequenceN(partitions.size)(partitions.map(function))

  private def processAllPerspectives(controller: PerspectiveController): fs2.Stream[F, QueryManagement] = {
    val firstPerspective = controller.nextPerspective()
    for {
      perspective <- fs2.Stream
                       .iterateEval(firstPerspective)(_ => Async[F].delay(controller.nextPerspective()))
                       .takeWhile(_.nonEmpty)
                       .map(_.get)
      result      <- fs2.Stream.eval(processPerspective(perspective))
      message     <- fs2.Stream.fromOption(result._1) ++ fs2.Stream(result._2)
    } yield message
  }

  private def processPerspective(perspective: Perspective): F[(Option[PerspectiveResult], QueryManagement)] = {
    val completed: QueryManagement                                             = PerspectiveCompleted(perspective)
    val perspectiveProcessing: F[(Option[PerspectiveResult], QueryManagement)] = for {
      counts    <- partitionFunction(_.establishPerspective(PerspectiveCommand(graphId, jobId, perspective)))
      totalCount = counts.map(_.count).sum
      state      = GraphStateImplementation(totalCount)
      _         <- partitionFunction(_.setMetadata(NodeCount(graphId, jobId, totalCount)))
      _         <- processOperations(query, state)
      result    <- query.sink match {
                     case Some(TableOutputSink(_)) =>
                       partitionFunction(_.getResult(QueryId(graphId, jobId)))
                         .flatMap(results =>
                           mergePartitionResults(perspective, results).map(result => ((Some(result), completed)))
                         )
                     case _                        =>
                       partitionFunction(_.writePerspective(PerspectiveCommand(graphId, jobId, perspective)))
                         .as((None, completed))
                   }
    } yield result

    perspectiveProcessing
      .handleErrorWith { e =>
        val msg = s"Deployment '$graphId': Failed to handle message. ${e.getMessage}. Skipping perspective."
        F.delay(logger.error(msg, e)).as((None, PerspectiveFailed(perspective, e.getMessage)))
      }
  }

  private def processOperations(query: Query, state: GraphStateImplementation) =
    query.operations.zipWithIndex.map {
      case (operation, index) =>
        operation match {
          case SetGlobalState(fun)           => F.delay(fun(state))
          case _: GlobalTabularisingFunction => executeWithState(index, state, update = false)
          case it: IterateWithGraph[_]       => executeWithStateUntilConsensus(index, state, update = true, it.iterations)
          case _: GlobalGraphFunction        => executeWithState(index, state, update = true)
          case it: Iterate[_]                => executeUntilConsensus(index, it.iterations)
          case x                             => execute(index)
        }
    }.sequence_

  private def executeWithState(index: Int, state: GraphStateImplementation, update: Boolean) =
    executeWithStateUntilConsensus(index, state, update, 1)

  private def executeWithStateUntilConsensus(
      index: Int,
      state: GraphStateImplementation,
      update: Boolean,
      iterations: Int = 1
  ): F[Unit] =
    if (iterations <= 0) F.unit
    else
      for {
        results <-
          partitionFunction(partition =>
            // TODO: this is to avoid sharing the same object in local setups, but there should be a better solution because in remote mode we are serialising twice
            F.delay(kryo.deserialise[GraphStateImplementation](kryo.serialise(state)))
              .flatMap(state => partition.executeOperationWithState(OperationAndState(graphId, jobId, index, state)))
          )
        _       <- F.delay(if (update) {
                     results.foreach(result => state.update(result.state))
                     state.rotate()
                   })
        _       <- if (results.forall(result => result.voteToContinue)) F.unit
                   else executeWithStateUntilConsensus(index, state, update, iterations - 1)
      } yield ()

  private def execute(index: Int) = executeUntilConsensus(index, 1)

  private def executeUntilConsensus(index: Int, iterations: Int): F[Unit] =
    if (iterations <= 0) F.unit
    else
      for {
        results <- partitionFunction(_.executeOperation(Operation(graphId, jobId, index)))
        _       <- if (results.forall(result => result.voteToContinue)) F.unit
                   else executeUntilConsensus(index, iterations - 1)
      } yield ()

  private def mergePartitionResults(perspective: Perspective, results: Seq[PartitionResult]): F[PerspectiveResult] =
    Async[F]
      .delay(results.foldLeft(Array[Row]())((acc, partResult) => acc ++ partResult.rows))
      .map(rows => PerspectiveResult(perspective, partitions.size, rows))
}

object QueryHandlerF {

  def apply[F[_]: Async](
      firstTimestamp: Long,
      lastTimestamp: Long,
      partitions: Seq[PartitionService[F]],
      query: Query
  ): F[fs2.Stream[F, protocol.QueryManagement]] =
    new QueryHandlerF[F](query.graphID, query, partitions)
      .processQuery(firstTimestamp, lastTimestamp)
      .map(_.map(j => protocol.QueryManagement(j)))
}
