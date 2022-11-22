package com.raphtory.internals.components.partition

import cats.effect.Async
import cats.effect.std.Dispatcher
import cats.effect.std.Queue
import cats.effect.std.Semaphore
import cats.syntax.all._
import com.raphtory.api.analysis.graphstate.GraphState
import com.raphtory.api.analysis.graphstate.GraphStateImplementation
import com.raphtory.api.analysis.graphview.ClearChain
import com.raphtory.api.analysis.graphview.DirectedView
import com.raphtory.api.analysis.graphview.ExplodeSelect
import com.raphtory.api.analysis.graphview.ExplodeSelectWithGraph
import com.raphtory.api.analysis.graphview.GlobalSelect
import com.raphtory.api.analysis.graphview.GraphFunction
import com.raphtory.api.analysis.graphview.Iterate
import com.raphtory.api.analysis.graphview.IterateWithGraph
import com.raphtory.api.analysis.graphview.MultilayerView
import com.raphtory.api.analysis.graphview.ReduceView
import com.raphtory.api.analysis.graphview.ReversedView
import com.raphtory.api.analysis.graphview.Select
import com.raphtory.api.analysis.graphview.SelectWithGraph
import com.raphtory.api.analysis.graphview.Step
import com.raphtory.api.analysis.graphview.StepWithGraph
import com.raphtory.api.analysis.graphview.UndirectedView
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.TableFunction
import com.raphtory.api.analysis.visitor.Vertex
import com.raphtory.api.output.sink.Sink
import com.raphtory.api.output.sink.SinkExecutor
import com.raphtory.internals.components.querymanager.GenericVertexMessage
import com.raphtory.internals.components.querymanager.GraphFunctionCompleteWithState
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.components.querymanager.TableBuilt
import com.raphtory.internals.components.querymanager.TableFunctionComplete
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.graph.LensInterface
import com.raphtory.internals.graph.Perspective
import com.raphtory.internals.management.Partitioner
import com.raphtory.internals.management.Scheduler
import com.raphtory.protocol.Empty
import com.raphtory.protocol.GraphId
import com.raphtory.protocol.Operation
import com.raphtory.protocol.OperationAndState
import com.raphtory.protocol.OperationResult
import com.raphtory.protocol.OperationWithStateResult
import com.raphtory.protocol.PartitionResult
import com.raphtory.protocol.PartitionService
import com.raphtory.protocol.PerspectiveCommand
import com.raphtory.protocol.VertexMessages
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

class QueryExecutorF[F[_]](
    query: Query,
    partitionID: Int,
    storage: GraphPartition,
    partitions: Map[Int, PartitionService[F]],
    graphLens: Queue[F, LensInterface],
    sinkExecutor: SinkExecutor,
    messageBuffer: ArrayBuffer[GenericVertexMessage[_]],
    conf: Config
)(implicit F: Async[F]) {
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))
  private val loggingPrefix  = s"${query.name}_$partitionID:"

  def receiveMessages(messages: Seq[GenericVertexMessage[_]]): F[Empty] =
    withLens(lens => F.delay(messages.foreach(msg => lens.receiveMessage(msg))))

  def establishPerspective(perspective: Perspective): F[Empty] = {
    val (timestamp, window)                          = (perspective.timestamp, perspective.window)
    val (start, end)                                 = (perspective.actualStart, perspective.actualEnd)
    val sendMessage: GenericVertexMessage[_] => Unit = message => messageBuffer.addOne(message)
    timed(s"Creating perspective at time '$timestamp' with window '$window") {
      for {
        lens <- F.delay(storage.lens(query.name, start, end, 0, sendMessage, null, new Scheduler))
        _    <- graphLens.offer(lens)
      } yield Empty()
    }
  }

  def setMetada(count: Int): F[Empty] =
    timed(s"Setting metadata")(withLens(lens => F.delay(lens.setFullGraphSize(count))))

  def executeOperation(number: Int): F[OperationResult] = {
    val function = query.operations(number)
    timed(s"Executing operation $function") {
      withLens { lens =>
        function match {
          case function: GraphFunction =>
            F.delay(lens.nextStep()) *> executeGraphFunction(function, lens)
          case function: TableFunction =>
            F.delay(lens.nextStep()) *> executeTableFunction(function) as OperationResult(voteToContinue = true)
        }
      }
    }
  }

  def executeOperationWithState(number: Int, state: GraphStateImplementation): F[OperationWithStateResult] = {
    val function = query.operations(number).asInstanceOf[GraphFunction]
    timed(s"Executing operation $function") {
      withLens { lens =>
        F.delay(lens.nextStep()) *> executeGraphFunctionWithState(function, state, lens)
      }
    }
  }

  def getResult: F[PartitionResult] =
    timed(s"$loggingPrefix Getting results from table") {
      withLens { lens =>
        for {
          buffer <- F.delay(ArrayBuffer[Row]())
          _      <- withFinishCallback(cb => lens.writeDataTable(row => buffer.addOne(row))(cb))
          result <- F.delay(PartitionResult(buffer.toSeq))
        } yield result
      }
    }

  def writePerspective(perspective: Perspective): F[Empty] =
    timed(s"$loggingPrefix Writing results from table to sink ${query.sink.get.getClass.getSimpleName}") { // TODO unsafe call to get
      withLens { lens =>
        for {
          _ <- F.delay(sinkExecutor.setupPerspective(perspective))
          _ <- withFinishCallback(cb => lens.writeDataTable(row => sinkExecutor.threadSafeWriteRow(row))(cb))
          _ <- F.delay(sinkExecutor.closePerspective())
        } yield ()
      }
    }

  def endQuery: F[Empty] =
    for {
      _ <- F.delay(sinkExecutor.close())
      _ <- F.delay(logger.debug(s"$loggingPrefix Received command to end the query"))
    } yield Empty()

  private def executeGraphFunction(function: GraphFunction, lens: LensInterface): F[OperationResult] = {
    def processIterate    =
      withFinishCallback { cb =>
        function match {
          case Iterate(f: (Vertex => Unit) @unchecked, iterations, executeMessagedOnly) =>
            if (executeMessagedOnly)
              lens.runMessagedGraphFunction(f)(cb)
            else
              lens.runGraphFunction(f)(cb)
        }
      }
    def processNonIterate =
      withFinishCallback { cb =>
        function match {
          case MultilayerView(interlayerEdgeBuilder)                         => lens.explodeView(interlayerEdgeBuilder)(cb)
          case Step(f: (Vertex => Unit) @unchecked)                          => lens.runGraphFunction(f)(cb)
          case UndirectedView()                                              => lens.viewUndirected()(cb)
          case DirectedView()                                                => lens.viewDirected()(cb)
          case ReversedView()                                                => lens.viewReversed()(cb)
          case ClearChain()                                                  => lens.clearMessages(); cb
          case op: Select[Vertex] @unchecked                                 => lens.executeSelect(op.f)(cb)
          case op: ExplodeSelect[Vertex] @unchecked                          => lens.explodeSelect(op.f)(cb)
          case ReduceView(defaultMergeStrategy, mergeStrategyMap, aggregate) =>
            lens.reduceView(defaultMergeStrategy, mergeStrategyMap, aggregate)(cb)
        }
      }

    function match {
      case _: Iterate[_] =>
        wrapMessagingFunction(processIterate, lens) *> F.delay(OperationResult(voteToContinue = lens.checkVotes()))
      case _             => wrapMessagingFunction(processNonIterate, lens) as OperationResult(voteToContinue = true)
    }
  }

  private def executeGraphFunctionWithState(
      function: GraphFunction,
      graphState: GraphState,
      lens: LensInterface
  ): F[OperationWithStateResult] = {
    def processIterate    =
      withFinishCallback { cb =>
        function match {
          case iwg: IterateWithGraph[Vertex] @unchecked =>
            if (iwg.executeMessagedOnly)
              lens.runMessagedGraphFunction(iwg.f, graphState)(cb)
            else
              lens.runGraphFunction(iwg.f, graphState)(cb)
        }
      }
    def processNonIterate =
      withFinishCallback { cb =>
        function match {
          case StepWithGraph(f)                               => lens.runGraphFunction(f, graphState)(cb)
          case SelectWithGraph(f)                             => lens.executeSelect(f, graphState)(cb)
          case esf: ExplodeSelectWithGraph[Vertex] @unchecked => lens.explodeSelect(esf.f, graphState)(cb)
          case GlobalSelect(f)                                => if (partitionID == 0) lens.executeSelect(f, graphState)(cb) else cb
        }
      }

    function match {
      case _: IterateWithGraph[_] =>
        wrapMessagingFunction(processIterate, lens) *> F.delay(OperationWithStateResult(lens.checkVotes(), graphState))
      case _                      =>
        wrapMessagingFunction(processNonIterate, lens) as OperationWithStateResult(voteToContinue = true, graphState)
    }
  }

  private def executeTableFunction(function: TableFunction): F[Unit] = ???

  private def timed[A](processName: String)(f: F[A]) =
    for {
      output   <- F.timed(f)
      (time, a) = output
      _        <- F.delay(logger.debug(s"$loggingPrefix $processName took ${time.toMillis} ms to complete"))
    } yield a

  private def withLens[A](f: LensInterface => F[A]): F[A] =
    F.bracket(graphLens.take)(f)(lens => graphLens.offer(lens))

  private def withFinishCallback(function: (() => Unit) => Unit): F[Unit] =
    Dispatcher[F].use { dispatcher =>
      for {
        finish <- Semaphore[F](0)
        _      <- F.delay(function(() => dispatcher.unsafeRunSync(finish.release)))
        _      <- finish.acquire
      } yield ()
    }

  private def wrapMessagingFunction(f: F[Unit], lens: LensInterface) = clearBuffer *> f *> sendMessages(lens)

  private def sendMessages(lens: LensInterface) =
    for {
      partitioned <- F.delay(messageBuffer.groupBy(message => partitionForMessage(message)))
      delivery     = partitioned.map {
                       case (destination, messages) =>
                         if (destination == partitionID)
                           F.delay(messages.foreach(message => lens.receiveMessage(message)))
                         else
                           for {
                             chunk <- F.delay(messages.toSeq.asInstanceOf[Seq[GenericVertexMessage[Any]]])
                             _     <- if (chunk.nonEmpty)
                                        partitions(destination).receiveMessages(VertexMessages(query.graphID, chunk))
                                      else F.unit
                           } yield ()
                     }
      _           <- F.parSequenceN(partitions.size)(delivery.toSeq)
    } yield ()

  private def clearBuffer = F.delay(messageBuffer.clear())

  private def partitionForMessage(message: GenericVertexMessage[_]) = {
    val vId = message.vertexId match {
      case (v: Long, _) => v
      case v: Long      => v
    }
    Partitioner().getPartitionForId(vId)
  }
}

object QueryExecutorF {

  def apply[F[_]](
      query: Query,
      partitionID: Int,
      storage: GraphPartition,
      partitions: Map[Int, PartitionService[F]],
      conf: Config
  )(implicit F: Async[F]): F[QueryExecutorF[F]] =
    for {
      lens         <- Queue.unbounded[F, LensInterface]
      buffer       <- F.delay(ArrayBuffer[GenericVertexMessage[_]]())
      sinkExecutor <- F.delay(query.sink.get.executor(query.name, partitionID, conf)) // TODO: handle this as a resource
      executor     <-
        F.delay(new QueryExecutorF[F](query, partitionID, storage, partitions, lens, sinkExecutor, buffer, conf))
    } yield executor
}
