package com.raphtory.internals.components.partition

import com.raphtory.api.analysis.graphstate.GraphState
import com.raphtory.api.analysis.graphstate.GraphStateImplementation
import com.raphtory.api.analysis.graphview._
import com.raphtory.api.analysis.table.Explode
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.TableFilter
import com.raphtory.api.analysis.table.WriteToOutput
import com.raphtory.api.analysis.visitor.Vertex
import com.raphtory.api.output.sink.Sink
import com.raphtory.api.output.sink.SinkExecutor
import com.raphtory.internals.communication.EndPoint
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.querymanager._
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.graph.LensInterface
import com.raphtory.internals.graph.Perspective
import com.raphtory.internals.management.Scheduler
import com.raphtory.internals.management.python.EmbeddedPython
import com.raphtory.internals.storage.pojograph.PojoGraphLens
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.CompletableFuture
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable

private[raphtory] class QueryExecutor(
    partitionID: Int,
    sink: Sink,
    storage: GraphPartition,
    jobID: String,
    conf: Config,
    topics: TopicRepository,
    scheduler: Scheduler,
    pyScript: Option[String]
) extends Component[QueryManagement](conf) {

  private val logger: Logger                   =
    Logger(LoggerFactory.getLogger(this.getClass))
  logger.info(logMessage(s"Starting QueryExecutor."))
  private var currentPerspectiveID: Int        = _
  private var currentPerspective: Perspective  = _
  private var graphLens: LensInterface         = _
  private val sentMessageCount: AtomicLong     = new AtomicLong(0)
  private val receivedMessageCount: AtomicLong = new AtomicLong(0)
  private var votedToHalt: Boolean             = false

  private val msgBatchPath: String  = "raphtory.partitions.batchMessages"
  private val messageBatch: Boolean = conf.getBoolean(msgBatchPath)
  private val maxBatchSize: Int     = conf.getInt("raphtory.partitions.maxMessageBatchSize")

  private val sync = new QuerySuperstepSync(totalPartitions)
  pyScript.map(s => EmbeddedPython.global.run(s))

  private val sinkExecutor: SinkExecutor = sink.executor(jobID, partitionID, conf, topics)

  if (messageBatch)
    logger.debug(
            logMessage(
                    s"Message batching is set to on. To change this modify '$msgBatchPath' in the application conf."
            )
    )

  private val listener = topics.registerListener(
          s"$graphID-$jobID-query-executor-$partitionID",
          handleMessage,
          topics.jobOperations(jobID),
          partitionID
  )
  logger.debug(logMessage("Component message listener registered."))

  private val vertexMessageListener =
    if (totalPartitions > 1)
      Some(
              topics.registerListener(
                      s"$graphID-$jobID-query-executor-$partitionID",
                      receiveVertexMessage,
                      topics.vertexMessages(jobID),
                      partitionID
              )
      )
    else None
  logger.debug(logMessage("Vertex message listener registered."))

  private val vertexControlMessageListener =
    if (totalPartitions > 1)
      Some(
              topics.registerListener(
                      s"$graphID-$jobID-query-executor-$partitionID",
                      receiveVertexControlMessage,
                      topics.vertexMessagesSync(jobID),
                      partitionID
              )
      )
    else
      None
  logger.debug(logMessage("Vertex control message listener registered."))

  private val taskManager = topics.jobStatus(jobID).endPoint
  logger.debug(logMessage("TaskManager endpoint created"))

  private val neighbours: Map[Int, EndPoint[VertexMessaging]] =
    if (totalPartitions > 1)
      topics.vertexMessages(jobID).endPoint
    else
      Map.empty
  logger.debug(logMessage("Vertex message endpoints created"))

  private val syncNeighbours: Map[Int, EndPoint[VertexMessagesSync]] = {
    if (totalPartitions > 1)
      topics.vertexMessagesSync(jobID).endPoint
    else
      Map.empty
  }
  logger.debug(logMessage("Vertex control message endpoints created."))

  override def run(): Unit = {
    logger.debug(logMessage(s"Starting query executor consumer."))
    listener.start()
    logger.debug(logMessage("Query executor consumer started, starting vertex message listeners."))
    vertexMessageListener.foreach(_.start())
    logger.debug(logMessage("Vertex message listeners started, starting vertex control message listeners."))
    vertexControlMessageListener.foreach(_.start())
    logger.debug(logMessage("Vertex control message listeners started."))
    taskManager sendAsync ExecutorEstablished(partitionID)
    logger.debug(logMessage("QueryExecutor initialised."))
  }

  override def stop(): Unit = {
    listener.close()
    vertexMessageListener.foreach(_.close())
    vertexControlMessageListener.foreach(_.close())
    logger.debug(logMessage(s"closing query executor consumer."))
    taskManager.close()
    neighbours.values.foreach(_.close())
    syncNeighbours.values.foreach(_.close())
  }

  def receiveVertexMessage(msg: VertexMessaging): Unit =
    try msg match {

      case VertexMessageBatch(msgBatch) =>
        logger.trace(
                logMessage(
                        s"Executing 'VertexMessageBatch', '[${msgBatch
                          .mkString(",")}]'."
                )
        )
        msgBatch.foreach(message => graphLens.receiveMessage(message))
        receivedMessageCount.addAndGet(msgBatch.size)
        sync.updateVertexMessageCount(msgBatch.size)

      case msg: GenericVertexMessage[_] =>
        logger.trace(
                logMessage(
                        s"Executing 'VertexMessage', '$msg'."
                )
        )
        graphLens.receiveMessage(msg)
        receivedMessageCount.incrementAndGet()
        sync.updateVertexMessageCount(1)
    }
    catch {
      case e: Throwable =>
        errorHandler(e)
    }

  def receiveVertexControlMessage(msg: VertexMessagesSync): Unit = {
    logger.debug(
            logMessage(
                    s"Received control message from ${msg.partitionID}, should receive ${msg.count} messages"
            )
    )
    sync.updateControlMessageCount(msg.count)
  }

  override def handleMessage(msg: QueryManagement): Unit = {
    val time = System.currentTimeMillis()
    try {
      msg match {
        case CreatePerspective(id, perspective)                                       =>
          currentPerspectiveID = id
          currentPerspective = perspective
          receivedMessageCount.set(0)
          sentMessageCount.set(0)
          sync.reset()
          refreshBuffers()
          graphLens = PojoGraphLens(
                  jobID,
                  perspective.actualStart,
                  perspective.actualEnd,
                  superStep = 0,
                  storage,
                  conf,
                  sendMessage,
                  errorHandler,
                  scheduler
          )

          taskManager sendAsync PerspectiveEstablished(currentPerspectiveID, graphLens.localNodeCount)
          logger.debug(
                  logMessage(
                          s"Created perspective at time '${perspective.timestamp}' with window '${perspective.window}'. in ${System
                            .currentTimeMillis() - time}ms"
                  )
          )

        case SetMetaData(vertices)                                                    =>
          graphLens.setFullGraphSize(vertices)
          taskManager sendAsync MetaDataSet(currentPerspectiveID)
          logger.debug(
                  logMessage(
                          s"Meta Data set in ${System.currentTimeMillis() - time}ms"
                  )
          )

        case GraphFunctionWithGlobalState(function, graphState)                       =>
          function match {
            case StepWithGraph(f)                                     =>
              evalStepWithGraph(time, graphState, f)

            case IterateWithGraph(f, iterations, executeMessagedOnly) =>
              startStep()
              val fun =
                if (executeMessagedOnly)
                  graphLens.runMessagedGraphFunction(f, graphState)(_)
                else
                  graphLens.runGraphFunction(f, graphState)(_)
              fun {
                finaliseStep {
                  val sentMessages     = sentMessageCount.get()
                  val receivedMessages = receivedMessageCount.get()
                  votedToHalt = graphLens.checkVotes()
                  taskManager sendAsync
                    GraphFunctionCompleteWithState(
                            currentPerspectiveID,
                            partitionID,
                            receivedMessages,
                            sentMessages,
                            votedToHalt,
                            graphState
                    )

                  logger.debug(
                          logMessage(
                                  s"Iterate function on graph with accumulators completed  in ${System
                                    .currentTimeMillis() - time}ms and sent '$sentMessages' messages with `executeMessageOnly` flag set to $executeMessagedOnly."
                          )
                  )
                }
              }

            case SelectWithGraph(f)                                   =>
              startStep()
              graphLens.executeSelect(f, graphState) {
                finaliseStep {
                  taskManager sendAsync TableBuilt(currentPerspectiveID)
                  logger.debug(
                          logMessage(
                                  s"Select executed on graph with accumulators in ${System
                                    .currentTimeMillis() - time}ms."
                          )
                  )
                }
              }
            case GlobalSelect(f)                                      =>
              evalGlobalSelect(time, graphState, f)
          }

        case MultilayerView(interlayerEdgeBuilder)                                    =>
          startStep()
          graphLens.explodeView(interlayerEdgeBuilder) {
            finaliseStep {
              val sentMessages     = sentMessageCount.get()
              val receivedMessages = receivedMessageCount.get()
              taskManager sendAsync
                GraphFunctionComplete(
                        currentPerspectiveID,
                        partitionID,
                        receivedMessages,
                        sentMessages
                )

              logger
                .debug(logMessage(s"MultilayerView function finished in ${System
                  .currentTimeMillis() - time}ms and sent '$sentMessages' messages."))
            }
          }

        case ReduceView(defaultMergeStrategy, mergeStrategyMap, aggregate)            =>
          startStep()
          graphLens.reduceView(defaultMergeStrategy, mergeStrategyMap, aggregate) {
            finaliseStep {
              val sentMessages     = sentMessageCount.get()
              val receivedMessages = receivedMessageCount.get()

              taskManager sendAsync
                GraphFunctionComplete(
                        currentPerspectiveID,
                        partitionID,
                        receivedMessages,
                        sentMessages
                )

              logger
                .debug(logMessage(s"MultilayerView function finished in ${System
                  .currentTimeMillis() - time}ms and sent '$sentMessages' messages."))
            }
          }

        case Step(f: (Vertex => Unit) @unchecked)                                     =>
          evaluateStep(time, f)

        case UndirectedView()                                                         =>
          startStep()
          graphLens.viewUndirected() {
            finaliseStep {
              val sentMessages     = sentMessageCount.get()
              val receivedMessages = receivedMessageCount.get()

              taskManager sendAsync
                GraphFunctionComplete(
                        currentPerspectiveID,
                        partitionID,
                        receivedMessages,
                        sentMessages
                )

              logger
                .debug(logMessage(s"UndirectedView function finished in ${System
                  .currentTimeMillis() - time}ms and sent '$sentMessages' messages."))
            }
          }

        case DirectedView()                                                           =>
          startStep()
          graphLens.viewDirected() {
            finaliseStep {
              val sentMessages     = sentMessageCount.get()
              val receivedMessages = receivedMessageCount.get()

              taskManager sendAsync
                GraphFunctionComplete(
                        currentPerspectiveID,
                        partitionID,
                        receivedMessages,
                        sentMessages
                )

              logger
                .debug(logMessage(s"DirectedView function finished in ${System
                  .currentTimeMillis() - time}ms and sent '$sentMessages' messages."))
            }
          }

        case Iterate(f: (Vertex => Unit) @unchecked, iterations, executeMessagedOnly) =>
          evaluateIterate(time, f, executeMessagedOnly)

        case ReversedView()                                                           =>
          startStep()
          graphLens.viewReversed() {
            finaliseStep {
              val sentMessages     = sentMessageCount.get()
              val receivedMessages = receivedMessageCount.get()

              taskManager sendAsync
                GraphFunctionComplete(
                        currentPerspectiveID,
                        partitionID,
                        receivedMessages,
                        sentMessages
                )

              logger
                .debug(logMessage(s"ReversedView function finished in ${System
                  .currentTimeMillis() - time}ms and sent '$sentMessages' messages."))
            }
          }

        case ClearChain()                                                             =>
          graphLens.clearMessages()
          taskManager sendAsync GraphFunctionComplete(currentPerspectiveID, partitionID, 0, 0)
          logger.debug(
                  logMessage(
                          s"Messages cleared on graph in ${System
                            .currentTimeMillis() - time}ms."
                  )
          )

        case Select(f)                                                                =>
          startStep()
          graphLens.executeSelect(f) {
            finaliseStep {
              taskManager sendAsync TableBuilt(currentPerspectiveID)
              logger.debug(
                      logMessage(
                              s"Select executed on graph in ${System
                                .currentTimeMillis() - time}ms."
                      )
              )
            }
          }

        //TODO create explode select with accumulators
        case ExplodeSelect(f)                                                         =>
          startStep()
          graphLens.explodeSelect(f) {
            finaliseStep {
              taskManager sendAsync TableBuilt(currentPerspectiveID)
              logger.debug(
                      logMessage(
                              s"Exploded Select executed on graph in ${System
                                .currentTimeMillis() - time}ms."
                      )
              )
            }
          }

        case TableFilter(f)                                                           =>
          graphLens.filteredTable(f) {
            taskManager sendAsync TableFunctionComplete(currentPerspectiveID)
            logger.debug(
                    logMessage(
                            s"Table Filter executed on table in ${System
                              .currentTimeMillis() - time}ms."
                    )
            )
          }

        case Explode(f)                                                               =>
          graphLens.explodeTable(f) {
            taskManager sendAsync TableFunctionComplete(currentPerspectiveID)
            logger.debug(
                    logMessage(
                            s"Table Explode executed on table in ${System
                              .currentTimeMillis() - time}ms."
                    )
            )
          }

        case WriteToOutput                                                            =>
          sinkExecutor.setupPerspective(currentPerspective)
          val writer = row => sinkExecutor.threadSafeWriteRow(row)
          graphLens.writeDataTable(writer) {
            sinkExecutor.closePerspective()
            taskManager sendAsync TableFunctionComplete(currentPerspectiveID)
            logger.debug(
                    logMessage(
                            s"Writing Results executed on table in ${System
                              .currentTimeMillis() - time}ms. Results written to '${sink.getClass.getSimpleName}'."
                    )
            )
          }

        case CompleteWrite                                                            =>
          sinkExecutor.close()
          logger.debug(
                  logMessage(
                          "Received 'CompleteWrite' message. Output writer was successfully closed."
                  )
          )
          taskManager sendAsync WriteCompleted

        //TODO Kill this worker once this is received
        case EndQuery(jobID)                                                          =>
          logger.debug(logMessage("Received 'EndQuery' message. "))
      }
    }
    catch {
      case e: Throwable =>
        errorHandler(e)
    }
    logger.debug(logMessage(s"Handled message $msg in ${System.currentTimeMillis() - time}ms"))
  }

  private def evalGlobalSelect(time: Long, graphState: GraphStateImplementation, f: GraphState => Row): Unit = {
    startStep()
    if (partitionID == 0)
      graphLens.executeSelect(f, graphState) {
        taskManager sendAsync TableBuilt(currentPerspectiveID)
        logger.debug(
                logMessage(
                        s"Global Select executed on graph with accumulators in ${System
                          .currentTimeMillis() - time}ms."
                )
        )
      }
    else {
      taskManager sendAsync TableBuilt(currentPerspectiveID)
      logger.debug(
              logMessage(
                      s"Global Select executed on graph with accumulators in ${System
                        .currentTimeMillis() - time}ms."
              )
      )
    }
  }

  private def evalStepWithGraph(time: Long, graphState: GraphStateImplementation, f: (_, GraphState) => Unit): Unit = {
    startStep()
    graphLens.runGraphFunction(f, graphState) {
      finaliseStep {
        val sentMessages     = sentMessageCount.get()
        val receivedMessages = receivedMessageCount.get()
        taskManager sendAsync
          GraphFunctionCompleteWithState(
                  currentPerspectiveID,
                  partitionID,
                  receivedMessages,
                  sentMessages,
                  graphState = graphState
          )

        logger.debug(
                logMessage(
                        s"Step function on graph with accumulators finished in ${System
                          .currentTimeMillis() - time}ms and sent '$sentMessages' messages."
                )
        )
      }
    }
  }

  private def evaluateIterate(time: Long, f: Vertex => Unit, executeMessagedOnly: Boolean): Unit = {
    startStep()
    val fun =
      if (executeMessagedOnly)
        graphLens.runMessagedGraphFunction(f)(_)
      else
        graphLens.runGraphFunction(f)(_)
    fun {
      finaliseStep {
        val sentMessages     = sentMessageCount.get()
        val receivedMessages = receivedMessageCount.get()

        taskManager sendAsync
          GraphFunctionComplete(
                  currentPerspectiveID,
                  partitionID,
                  receivedMessages,
                  sentMessages,
                  graphLens.checkVotes()
          )

        votedToHalt = graphLens.checkVotes()
        logger.debug(
                logMessage(
                        s"Iterate function completed in ${System
                          .currentTimeMillis() - time}ms and sent '$sentMessages' messages with `executeMessageOnly` flag set to $executeMessagedOnly."
                )
        )
      }
    }
  }

  private def evaluateStep(time: Long, f: Vertex => Unit): Unit = {
    startStep()
    graphLens.runGraphFunction(f) {
      finaliseStep {
        val sentMessages     = sentMessageCount.get()
        val receivedMessages = receivedMessageCount.get()
        taskManager sendAsync
          GraphFunctionComplete(
                  currentPerspectiveID,
                  partitionID,
                  receivedMessages,
                  sentMessages
          )

        logger
          .debug(logMessage(s"Step function finished in ${System
            .currentTimeMillis() - time}ms and sent '$sentMessages' messages."))
      }
    }
  }

  def errorHandler(error: Throwable): Unit = {
    error.printStackTrace()
    taskManager sendAsync AlgorithmFailure(currentPerspectiveID, error)
  }

  private val messageCache: Map[Int, mutable.ArrayBuffer[GenericVertexMessage[_]]] =
    neighbours.keys.map(n => n -> mutable.ArrayBuffer.empty[GenericVertexMessage[_]]).toMap

  private val perStepSentMessageCounts: Map[Int, AtomicLong]                       = neighbours.keys.map(n => n -> new AtomicLong(0)).toMap

  def sendMessage(message: GenericVertexMessage[_]): Unit = {
    val vId                  = message.vertexId match {
      case (v: Long, _) => v
      case v: Long      => v
    }
    sentMessageCount.incrementAndGet()
    val destinationPartition = (vId.abs % totalPartitions).toInt
    if (destinationPartition == partitionID) { //sending to this partition
      graphLens.receiveMessage(message)
      receivedMessageCount.incrementAndGet()
    }
    else { //sending to a remote partition
      perStepSentMessageCounts(destinationPartition).incrementAndGet()
      val producer =
        try neighbours(destinationPartition)
        catch {
          case e: NoSuchElementException =>
            val msg =
              s"Trying to send message to partition $destinationPartition from partition $partitionID but no endPoints were provided"
            logger.error(logMessage(msg))
            throw new IllegalStateException(msg)
        }
      if (messageBatch) {
        val cache = messageCache(destinationPartition)
        cache.synchronized {
          cache += message
          if (cache.size > maxBatchSize)
            sendCached(destinationPartition)
        }
      }
      else
        producer sendAsync message
    }

  }

  def sendCached(partition: Int): Unit = {
    val cache = messageCache(partition)
    cache.synchronized {
      neighbours(partition) sendAsync VertexMessageBatch(cache.toArray)
      cache.clear() // synchronisation breaks if we create a new object here
    }
  }

  def flushMessages(): CompletableFuture[Void] = {
    logger.debug(logMessage("Flushing vertex messages."))

    if (messageBatch)
      messageCache.keys.foreach(producer => sendCached(producer))

    val futures = neighbours.values.map(_.flushAsync())
    CompletableFuture.allOf(futures.toSeq: _*)
  }

  def flushControlMessages(): CompletableFuture[Void] = {
    logger.debug(logMessage("Flushing control messages."))
    CompletableFuture.allOf(syncNeighbours.values.map(_.flushAsync()).toSeq: _*)
  }

  private def refreshBuffers(): Unit = {
    logger.debug(logMessage("Refreshing messageCache buffers for all Producers."))
    messageCache.values.foreach(_.clear())
  }

  private def startStep(): Unit = {
    graphLens.nextStep()
    perStepSentMessageCounts.values.foreach(_.set(0))
  }

  private def finaliseStep(f: => Unit): Unit = {
    syncNeighbours.foreach {
      case (id, endPoint) =>
        if (id != partitionID) {
          logger.debug(
                  logMessage(
                          s"Finished sending messages, final sent count for target $id is ${perStepSentMessageCounts(id).get()}"
                  )
          )
          endPoint.sendAsync(VertexMessagesSync(partitionID, perStepSentMessageCounts(id).get()))
        }
    }
    perStepSentMessageCounts.values.foreach(_.set(0))
    flushMessages()
      .thenCompose(_ => flushControlMessages())
      .thenCompose(_ => sync.awaitSuperstepComplete)
      .thenCompose(_ =>
        scheduler.executeCompletable {
          logger.debug(logMessage("Received all messages, finalising step"))
          f
        }
      )
  }

  private def logMessage(msg: String): String = s"${jobID}_$partitionID: $msg"
}

class QuerySuperstepSync(totalPartitions: Int) {
  private val logger: Logger                             = Logger(LoggerFactory.getLogger(this.getClass))
  private val controlMessageSemaphore                    = new Semaphore(0)
  private val actualReceivedMessageCount: AtomicLong     = new AtomicLong(0)
  private val targetReceivedMessageCount: AtomicLong     = new AtomicLong(0)
  private val receivedControlMessageCount: AtomicInteger = new AtomicInteger(0)

  def reset(): Unit = {
    actualReceivedMessageCount.set(0)
    targetReceivedMessageCount.set(0)
    receivedControlMessageCount.set(0)
  }

  def updateControlMessageCount(count: Long): Unit = {
    targetReceivedMessageCount.addAndGet(count)
    controlMessageSemaphore.release(1)
  }

  def updateVertexMessageCount(count: Int): Unit =
    actualReceivedMessageCount.addAndGet(count)

  def awaitSuperstepComplete: CompletableFuture[Void] =
    CompletableFuture.runAsync { () =>
      controlMessageSemaphore.acquire(totalPartitions - 1)
      val target = targetReceivedMessageCount.get()
      while (!actualReceivedMessageCount.compareAndSet(target, target)) {}
    }
}
