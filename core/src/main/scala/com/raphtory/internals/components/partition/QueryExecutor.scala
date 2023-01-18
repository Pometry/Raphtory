package com.raphtory.internals.components.partition

import cats.effect.Async
import cats.effect.Ref
import cats.effect.std.Dispatcher
import cats.syntax.all._
import com.google.protobuf.empty.Empty
import com.raphtory.api.analysis.graphstate.GraphState
import com.raphtory.api.analysis.graphstate.GraphStateImplementation
import com.raphtory.api.analysis.graphview._
import com.raphtory.api.analysis.table.Explode
import com.raphtory.api.analysis.table.ExplodeColumn
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.TableFilter
import com.raphtory.api.analysis.table.TableFunction
import com.raphtory.api.analysis.visitor.Vertex
import com.raphtory.api.output.sink.SinkExecutor
import com.raphtory.internals.components.querymanager.GenericVertexMessage
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.graph.LensInterface
import com.raphtory.internals.graph.Perspective
import com.raphtory.internals.management.Partitioner
import com.raphtory.internals.management.Scheduler
import com.raphtory.internals.serialisers.KryoSerialiser
import com.raphtory.protocol._
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

class QueryExecutor[F[_]](
    query: Query,
    partitionID: Int,
    storage: GraphPartition,
    partitions: Map[Int, PartitionService[F]],
    graphLens: Ref[F, Option[LensInterface]],
    sinkExecutor: SinkExecutor,
    messageBuffer: ArrayBuffer[GenericVertexMessage[_]],
    dispatcher: Dispatcher[F],
    conf: Config
)(implicit F: Async[F]) {
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))
  private val graphId        = query.graphID
  private val jobId          = query.name
  private val loggingPrefix  = s"${jobId}_$partitionID:"
  private val kryo           = KryoSerialiser()
  private val partitioner    = Partitioner(conf)
  private val cb: () => Unit = () => {}

  def receiveMessages(messages: Seq[GenericVertexMessage[_]]): F[Empty] =
    withLens(lens => F.delay(messages.foreach(msg => lens.receiveMessage(msg)))).as(Empty())

  def establishPerspective(perspective: Perspective): F[NodeCount] = {
    val (timestamp, window) = (perspective.timestamp, perspective.window)
    val (start, end)        = (perspective.actualStart, perspective.actualEnd)
    timed(s"Creating perspective at time '$timestamp' with window '$window, real start $start, real end $end") {
      for {
        lens <- F.delay(storage.lens(jobId, start, end, 0, unsafeSendMessage, null, new Scheduler))
        _    <- graphLens.update(_ => Some(lens))
      } yield NodeCount(count = lens.localNodeCount)
    }
  }

  def setMetadata(count: Int): F[Empty] =
    timed(s"Setting metadata")(withLens(lens => F.delay(lens.setFullGraphSize(count)))).as(Empty())

  def executeOperation(number: Int): F[OperationResult] = {
    val function = query.operations(number)
    timed(s"Executing operation ${function.getClass.getSimpleName}") {
      withLens { lens =>
        function match {
          case function: GraphFunction =>
            F.delay(lens.nextStep()) *> executeGraphFunction(function, lens)
          case function: TableFunction => // Table functions don't call nextStep
            executeTableFunction(function, lens) as OperationResult(voteToContinue = true)
        }
      }
    }
  }

  def executeOperationWithState(number: Int, state: GraphStateImplementation): F[OperationWithStateResult] = {
    val function = query.operations(number).asInstanceOf[GraphFunction]
    timed(s"Executing operation ${function.getClass.getSimpleName}") {
      withLens(lens => F.delay(lens.nextStep()) *> executeGraphFunctionWithState(function, state, lens))
    }
  }

  def getResult: F[PartitionResult] =
    timed(s"$loggingPrefix Getting results from table") {
      withLens { lens =>
        for {
          buffer <- F.delay(ArrayBuffer[Row]())
          _      <- F.blocking(lens.writeDataTable(row => buffer.addOne(row))(cb))
          result <- F.delay(PartitionResult(buffer.toSeq))
        } yield result
      }
    }

  def writePerspective(perspective: Perspective): F[Empty] =
    timed(s"Writing results from table to sink ${query.sink.get.getClass.getSimpleName}") { // TODO unsafe call to get
      withLens { lens =>
        F.bracket(F.delay(sinkExecutor.setupPerspective(perspective, query.header))) { _ =>
          F.blocking(lens.writeDataTable(row => sinkExecutor.threadSafeWriteRow(row))(cb))
        }(_ => F.delay(sinkExecutor.closePerspective()))
          .map(_ => Empty())
      }
    }

  def endQuery: F[Empty] =
    for {
      _ <- F.delay(sinkExecutor.close())
      _ <- F.delay(logger.debug(s"$loggingPrefix Received command to end the query"))
    } yield Empty()

  private def executeGraphFunction(function: GraphFunction, lens: LensInterface): F[OperationResult] = {
    def processIterate(f: Vertex => Unit, executeMessagedOnly: Boolean) =
      F.blocking(if (executeMessagedOnly) lens.runMessagedGraphFunction(f)(cb) else lens.runGraphFunction(f)(cb))

    def processNonMessaging =
      F.blocking(
              function match {
                case MultilayerView(interlayerEdgeBuilder) => lens.explodeView(interlayerEdgeBuilder)(cb)
                case Step(f: (Vertex => Unit) @unchecked)  => lens.runGraphFunction(f)(cb)
                case UndirectedView()                      => lens.viewUndirected()(cb)
                case DirectedView()                        => lens.viewDirected()(cb)
                case ReversedView()                        => lens.viewReversed()(cb)
                case ClearChain()                          => lens.clearMessages(); cb()
                case op: Select @unchecked                 => lens.executeSelect(op.values, query.defaults)(cb)
                case op: ExplodeSelect[Vertex] @unchecked  => lens.explodeSelect(op.f)(cb)
                case rv: ReduceView                        => lens.reduceView(rv.defaultMergeStrategy, rv.mergeStrategyMap, rv.aggregate)(cb)
                case x                                     => throw new Exception(s"$x not handled")
              }
      )

    function match {
      case Step(f: (Vertex => Unit) @unchecked) =>
        wrapMessagingFunction(F.blocking(lens.runGraphFunction(f)(cb)), lens) as OperationResult()
      case it: Iterate[Vertex] @unchecked       =>
        for {
          _      <- wrapMessagingFunction(processIterate(it.f, it.executeMessagedOnly), lens)
          result <- F.blocking(OperationResult(voteToContinue = lens.checkVotes()))
        } yield result
      case _                                    => processNonMessaging as OperationResult()
    }
  }

  private def executeGraphFunctionWithState(
      function: GraphFunction,
      graphState: GraphStateImplementation,
      lens: LensInterface
  ): F[OperationWithStateResult] = {
    def processIterate(f: ((Vertex, GraphState) => Unit), executeMessagedOnly: Boolean) =
      F.blocking(
              if (executeMessagedOnly) lens.runMessagedGraphFunction(f, graphState)(cb)
              else lens.runGraphFunction(f, graphState)(cb)
      )

    def processNonMessaging =
      F.blocking(
              function match {
                case SelectWithGraph(f)                             => lens.executeSelect(f, graphState)(cb)
                case esf: ExplodeSelectWithGraph[Vertex] @unchecked => lens.explodeSelect(esf.f, graphState)(cb)
                case GlobalSelect(f)                                => if (partitionID == 0) lens.executeSelect(f, graphState)(cb) else cb()
              }
      )

    function match {
      case StepWithGraph(f)                        =>
        for {
          _ <- wrapMessagingFunction(F.blocking(lens.runGraphFunction(f, graphState)(cb)), lens)
        } yield OperationWithStateResult(voteToContinue = true, graphState)
      case it: IterateWithGraph[Vertex] @unchecked =>
        for {
          _      <- wrapMessagingFunction(processIterate(it.f, it.executeMessagedOnly), lens)
          result <- F.blocking(OperationWithStateResult(lens.checkVotes(), graphState))
        } yield result
      case _                                       =>
        processNonMessaging as OperationWithStateResult(voteToContinue = true, graphState)
    }
  }

  private def executeTableFunction(function: TableFunction, lens: LensInterface): F[Unit] =
    F.blocking(
            function match {
              case TableFilter(f)        => lens.filteredTable(f)(cb)
              case Explode(f)            => lens.explodeTable(f)(cb)
              case ExplodeColumn(column) => lens.explodeColumn(column)(cb)
            }
    )

  private def timed[A](processName: String)(f: F[A]) =
    for {
      output   <- F.timed(f)
      (time, a) = output
      _        <- F.delay(logger.debug(s"$loggingPrefix $processName took ${time.toMillis} ms to complete"))
    } yield a

  private def withLens[A](f: LensInterface => F[A]): F[A] = graphLens.get.flatMap(lens => f(lens.get))

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
                                        partitions(destination).receiveMessages(VertexMessages(graphId, jobId, chunk))
                                      else F.unit
                           } yield ()
                     }
      _           <- F.parSequenceN(partitions.size)(delivery.toSeq)
    } yield ()

  private def clearBuffer = F.delay(messageBuffer.clear())

  private def unsafeSendMessage(message: GenericVertexMessage[_]): Unit =
    messageBuffer.synchronized {
      messageBuffer.addOne(message)
      if (messageBuffer.size >= 1024) {
        dispatcher.unsafeRunSync(for {
          lens <- graphLens.get
          _    <- sendMessages(lens.get)
        } yield ())
        messageBuffer.clear()
      }
    }

  private def partitionForMessage(message: GenericVertexMessage[_]) = {
    val vId = message.vertexId match {
      case (v: Long, _) => v
      case v: Long      => v
    }
    partitioner.getPartitionForId(vId)
  }
}

object QueryExecutor {

  def apply[F[_]](
      query: Query,
      id: Int,
      storage: GraphPartition,
      partitions: Map[Int, PartitionService[F]],
      dispatcher: Dispatcher[F],
      conf: Config
  )(implicit F: Async[F]): F[QueryExecutor[F]] =
    for {
      lens         <- Ref.of[F, Option[LensInterface]](None)
      buffer       <- F.delay(ArrayBuffer[GenericVertexMessage[_]]())
      sinkExecutor <- F.delay(query.sink.get.executor(query.name, id, conf)) // TODO: handle this as a resource
      executor     <-
        F.delay(new QueryExecutor[F](query, id, storage, partitions, lens, sinkExecutor, buffer, dispatcher, conf))
    } yield executor
}
