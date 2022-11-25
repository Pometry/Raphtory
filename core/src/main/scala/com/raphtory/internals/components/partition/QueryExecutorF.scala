package com.raphtory.internals.components.partition

import cats.effect.Async
import cats.effect.Ref
import cats.effect.std.Dispatcher
import cats.effect.std.Queue
import cats.effect.std.Semaphore
import cats.syntax.all._
import com.google.protobuf.empty.Empty
import com.raphtory.api.analysis.graphstate.AccumulatorImplementation
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
import com.raphtory.api.analysis.table.Explode
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
import com.raphtory.protocol.NodeCount
import com.raphtory.protocol.OperationResult
import com.raphtory.protocol.OperationWithStateResult
import com.raphtory.protocol.PartitionResult
import com.raphtory.protocol.PartitionService
import com.raphtory.protocol.VertexMessages
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt

class QueryExecutorF[F[_]](
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
  private val cb: () => Unit = () => {}

  def receiveMessages(messages: Seq[GenericVertexMessage[_]]): F[Empty] =
    withLens(lens => F.delay(messages.foreach(msg => lens.receiveMessage(msg)))).as(Empty())

  def establishPerspective(perspective: Perspective): F[NodeCount] = {
    val (timestamp, window) = (perspective.timestamp, perspective.window)
    val (start, end)        = (perspective.actualStart, perspective.actualEnd)
    timed(s"Creating perspective at time '$timestamp' with window '$window") {
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
    timed(s"Executing operation $function") {
      withLens { lens =>
        function match {
          case function: GraphFunction =>
            F.delay(lens.nextStep()) *> executeGraphFunction(function, lens)
          case function: TableFunction =>
            F.delay(lens.nextStep()) *> executeTableFunction(function, lens) as OperationResult(voteToContinue = true)
        }
      }
    }
  }

  def executeOperationWithState(number: Int, state: GraphStateImplementation): F[OperationWithStateResult] = {
    val function = query.operations(number).asInstanceOf[GraphFunction]
    timed(s"Executing operation $function") {
      withLens(lens => F.delay(lens.nextStep()) *> executeGraphFunctionWithState(function, state, lens))
    }
  }

  def getResult: F[PartitionResult] =
    timed(s"$loggingPrefix Getting results from table") {
      withLens { lens =>
        for {
          buffer <- F.delay(ArrayBuffer[Row]())
          // TODO Using kryo here is a hack so we get rows out of the row pool. Maybe we could consider if the pool mechanism is really necessary as it causes undesired behavior as in this case
          _      <- F.blocking(lens.writeDataTable(row => buffer.addOne(kryo.deserialise[Row](kryo.serialise(row))))(cb))
          _      <- F.delay(println(s"sending rows: ${buffer.toList}"))
          result <- F.delay(PartitionResult(buffer.toSeq))
        } yield result
      }
    }

  def writePerspective(perspective: Perspective): F[Empty] =
    timed(s"Writing results from table to sink ${query.sink.get.getClass.getSimpleName}") { // TODO unsafe call to get
      withLens { lens =>
        for {
          _ <- F.delay(sinkExecutor.setupPerspective(perspective))
          _ <- F.blocking(lens.writeDataTable(row => sinkExecutor.threadSafeWriteRow(row))(cb))
          _ <- F.delay(sinkExecutor.closePerspective())
        } yield Empty()
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

    def processNonIterate =
      F.blocking(
              function match {
                case MultilayerView(interlayerEdgeBuilder)                         => lens.explodeView(interlayerEdgeBuilder)(cb)
                case Step(f: (Vertex => Unit) @unchecked)                          => lens.runGraphFunction(f)(cb)
                case UndirectedView()                                              => lens.viewUndirected()(cb)
                case DirectedView()                                                => lens.viewDirected()(cb)
                case ReversedView()                                                => lens.viewReversed()(cb)
                case ClearChain()                                                  => lens.clearMessages(); cb()
                case op: Select[Vertex] @unchecked                                 => lens.executeSelect(op.f)(cb)
                case op: ExplodeSelect[Vertex] @unchecked                          => lens.explodeSelect(op.f)(cb)
                case ReduceView(defaultMergeStrategy, mergeStrategyMap, aggregate) =>
                  lens.reduceView(defaultMergeStrategy, mergeStrategyMap, aggregate)(cb)
                case x                                                             => throw new Exception(s"$x not handled")
              }
      )

    function match {
      case Iterate(f: (Vertex => Unit) @unchecked, _, executeMessagedOnly) =>
        wrapMessagingFunction(processIterate(f, executeMessagedOnly), lens) *> F.delay(
                OperationResult(voteToContinue = lens.checkVotes())
        )
      case _                                                               =>
        wrapMessagingFunction(processNonIterate, lens) as OperationResult(voteToContinue = true)
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

    def processNonIterate =
      F.blocking(
              function match {
                case StepWithGraph(f)                               => lens.runGraphFunction(f, graphState)(cb)
                case SelectWithGraph(f)                             => lens.executeSelect(f, graphState)(cb)
                case esf: ExplodeSelectWithGraph[Vertex] @unchecked => lens.explodeSelect(esf.f, graphState)(cb)
                case GlobalSelect(f)                                => if (partitionID == 0) lens.executeSelect(f, graphState)(cb) else cb()
                case x                                              => throw new Exception(s"$x not handled")
              }
      )

    function match {
      case IterateWithGraph(f: ((Vertex, GraphState) => Unit) @unchecked, _, executeMessagedOnly) =>
        wrapMessagingFunction(processIterate(f, executeMessagedOnly), lens) *> F.blocking(
                OperationWithStateResult(lens.checkVotes(), graphState)
        )
      case _                                                                                      =>
        wrapMessagingFunction(processNonIterate, lens) as OperationWithStateResult(voteToContinue = true, graphState)
    }
  }

  private def executeTableFunction(function: TableFunction, lens: LensInterface): F[Unit] =
    F.blocking(
            function match {
              case TableFilter(f) => lens.filteredTable(f)(cb)
              case Explode(f)     => lens.explodeTable(f)(cb)
            }
    )

  private def timed[A](processName: String)(f: F[A]) =
    for {
      output   <- F.timed(f)
      (time, a) = output
      _        <- F.delay(logger.info(s"$loggingPrefix $processName took ${time.toMillis} ms to complete"))
    } yield a

  private def withLens[A](f: LensInterface => F[A]): F[A] = graphLens.get.flatMap(lens => f(lens.get))

  private def wrapMessagingFunction(f: F[Unit], lens: LensInterface) = clearBuffer *> f *> sendMessages(lens)

  private def sendMessages(lens: LensInterface) =
    for {
//      _           <- F.delay(println(s"in sendMessages for partition $partitionID"))
      partitioned <- F.delay(messageBuffer.groupBy(message => partitionForMessage(message)))
//      _           <- F.delay(println(s"messages partitioned for partition $partitionID"))
      delivery     = partitioned.map {
                       case (destination, messages) =>
                         if (destination == partitionID)
//                           F.delay(println(s"receiving direct messages $partitionID")) *>
                           F.delay(messages.foreach(message => lens.receiveMessage(message)))
                         else
                           for {
//                             _     <- F.delay(println(s"about to send chunk $partitionID"))
                             chunk <- F.delay(messages.toSeq.asInstanceOf[Seq[GenericVertexMessage[Any]]])
                             _     <- if (chunk.nonEmpty)
                                        partitions(destination).receiveMessages(VertexMessages(graphId, jobId, chunk))
                                      else F.unit
                           } yield ()
                     }
//      _           <- F.delay(println(s"delivery defined for partition $partitionID"))
      _           <- F.parSequenceN(partitions.size)(delivery.toSeq)
    } yield ()

  private def clearBuffer = F.delay(messageBuffer.clear())

//  private def unsafeSendMessage(message: GenericVertexMessage[_]): Unit = {
//    if (message == null) println("adding null message !!!!!!!!!!!!!!!!")
//    messageBuffer.addOne(message)
//
//    if (messageBuffer.size >= 1024) {
//      println(s"entering sync block for $partitionID")
//      messageBuffer.synchronized { // This is being called in parallel. There might be a point where it isn't anymore
//        println(s"inside sync block $partitionID")
//        if (messageBuffer.size >= 1024) {
//          println(s"about to send chunk in $partitionID")
//          dispatcher.unsafeRunSync(graphLens.get.map(lens => sendMessages(lens.get)))
//          messageBuffer.clear()
//        }
//        println(s"getting out of syn block for $partitionID")
//      }
//    }
//  }

  private def unsafeSendMessage(message: GenericVertexMessage[_]): Unit =
    // println(s"entering sync block for $partitionID")
    messageBuffer.synchronized {
      // println(s"inside sync block $partitionID")
      messageBuffer.addOne(message)
      if (messageBuffer.size >= 1024) {
//        println(s"about to send chunk in $partitionID")
        dispatcher.unsafeRunSync(for {
//          _    <- F.delay(println(s"getting graphlens in $partitionID"))
          lens <- graphLens.get
//          _    <- F.delay(println(s"got graphlens in $partitionID"))
          _    <- sendMessages(lens.get)
        } yield ()) // graphLens.get.map(lens => sendMessages(lens.get)))
//        println(s"messages sent in $partitionID")
        messageBuffer.clear()
//        println(s"buffer clear in $partitionID")
      }
      // println(s"getting out of syn block for $partitionID")
    }

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
      id: Int,
      storage: GraphPartition,
      partitions: Map[Int, PartitionService[F]],
      dispatcher: Dispatcher[F],
      conf: Config
  )(implicit F: Async[F]): F[QueryExecutorF[F]] =
    for {
      lens         <- Ref.of[F, Option[LensInterface]](None)
      buffer       <- F.delay(ArrayBuffer[GenericVertexMessage[_]]())
      sinkExecutor <- F.delay(query.sink.get.executor(query.name, id, conf)) // TODO: handle this as a resource
      executor     <-
        F.delay(new QueryExecutorF[F](query, id, storage, partitions, lens, sinkExecutor, buffer, dispatcher, conf))
    } yield executor
}
