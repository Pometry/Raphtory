package com.raphtory.internals.components.querymanager

import cats.Parallel
import cats.effect.Async
import cats.syntax.all._
import com.raphtory.api.analysis.table.Row
import com.raphtory.internals.components.output.TableOutputSink
import com.raphtory.internals.graph.Perspective
import com.raphtory.internals.graph.PerspectiveController
import com.raphtory.protocol
import com.raphtory.protocol.GraphId
import com.raphtory.protocol.Operation
import com.raphtory.protocol.OperationAndState
import com.raphtory.protocol.PartitionResult
import com.raphtory.protocol.PartitionService
import com.raphtory.protocol.PerspectiveResult
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.util.Success

class QueryHandlerF[F[_]: Async](graphId: String, partitions: Seq[PartitionService[F]]) {

  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def processQuery(query: Query, firstTimestamp: Long, lastTimestamp: Long): F[fs2.Stream[F, QueryManagement]] = {
    val queryProcessing = for {
      _          <- partitionFunction(_.establishExecutor(protocol.Query(TryQuery(Success(query)))))
      controller <- Async[F].delay(PerspectiveController(firstTimestamp, lastTimestamp, query))
      responses  <- processAllPerspectives(query, controller).pure[F]
    } yield responses ++ fs2.Stream(JobDone)

    queryProcessing.handleErrorWith(e => Async[F].pure(fs2.Stream(JobFailed(e))))
  }

  private def partitionFunction[T](function: PartitionService[F] => F[T]) =
    Async[F].parSequenceN(partitions.size)(partitions.map(function))

  private def processAllPerspectives(
      query: Query,
      controller: PerspectiveController
  ): fs2.Stream[F, QueryManagement] = {
    val firstPerspective = controller.nextPerspective()
    for {
      perspectiveAndIndex <- fs2.Stream
                               .iterateEval(firstPerspective)(_ => Async[F].delay(controller.nextPerspective()))
                               .takeWhile(_.nonEmpty)
                               .map(_.get)
                               .zipWithIndex
      (perspective, index) = perspectiveAndIndex
      messages            <- fs2.Stream.eval(processPerspective(query, perspective, index.toInt))
      message             <- fs2.Stream.fromIterator(messages.iterator, 2)
    } yield message
  }

  private def processPerspective(
      query: Query,
      perspective: Perspective,
      index: Int
  ): F[List[QueryManagement]] = {
    val completed: QueryManagement = PerspectiveCompleted(perspective)
    val perspectiveProcessing      = for {
      _      <- partitionFunction(_.establishPerspective(protocol.CreatePerspective(graphId, index, perspective)))
      _      <- processOperations(query)
      result <- query.sink match {
                  case Some(TableOutputSink(_)) =>
                    partitionFunction(_.getResult(GraphId(graphId)))
                      .flatMap(results => mergePartitionResults(perspective, results).map(List(_, completed)))
                  case _                        =>
                    partitionFunction(_.writePerspective(GraphId(graphId))).as(List(completed))
                }
    } yield result

    perspectiveProcessing
      .handleErrorWith { e =>
        val msg = s"Deployment '$graphId': Failed to handle message. ${e.getMessage}. Skipping perspective."
        Async[F].delay(logger.error(msg, e)).as(fs2.Stream(PerspectiveFailed(perspective, e.getMessage)))
      }
  }

  private def processOperations(query: Query) =
    query.operations.zipWithIndex.map {
      case (operation, index) =>
        operation match {
          case GraphFunctionWithGlobalState(_, state) =>
            partitionFunction(_.executeOperationWithState(OperationAndState(graphId, index, state)))
          case _                                      =>
            partitionFunction(_.executeOperation(Operation(graphId, index)))
        }
    }.sequence_

  private def mergePartitionResults(perspective: Perspective, results: Seq[PartitionResult]): F[PerspectiveResult] =
    Async[F]
      .delay(results.foldLeft(Vector[Row]())((acc, partResult) => acc ++ partResult.rows))
      .map(rows => PerspectiveResult(perspective, partitions.size, rows))
}

object QueryHandlerF {

  def apply[F[_]: Async: Parallel](
      graphId: String,
      firstTimestamp: Long,
      lastTimestamp: Long,
      partitions: Seq[PartitionService[F]],
      query: Query
  ): F[fs2.Stream[F, QueryManagement]] =
    new QueryHandlerF[F](graphId, partitions).processQuery(query, firstTimestamp, lastTimestamp)
}
