package com.raphtory.core.components.graphbuilder

import com.raphtory.core.components.Component
import com.raphtory.core.config.PulsarController
import com.raphtory.serialisers.Marshal
import com.rits.cloning.Cloner
import com.typesafe.config.Config
import org.apache.pulsar.client.admin.PulsarAdminException
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.Schema
import shapeless.Coproduct

import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

/**
  * {s}`BuilderExecutor`
  *  : {s}`BuilderExecutor` is the entry-point for building graphs in Raphtory using {s}`GraphBuilder`. It is used to start the {s}`GraphBuilder`
  *    consumer and to send {s}`GraphUpdate` messages to the {s}`Writer`. {s}`GraphUpdate` messages are created while the {s}`GraphBuilder` adds vertices and edges
  *    to the graph.
  *
  * ## Methods
  *
  *   {s}`run(): Unit`
  *    : Starts the Graph Builder executor by starting the graph builder consumer
  *
  *   {s}`stop(): Unit`
  *    : Stops the Graph Builder executor by stopping the graph builder consumer and producers
  *
  *   {s}`handleMessage(msg: T): Unit`
  *    : Handles {s}`GraphUpdate` messages received by the graph builder consumer for each tuple or row of data parsed
  *
  *   {s}`sendUpdate(graphUpdate: GraphUpdate): Unit `
  *    : Sends serialised {s}`GraphUpdate` messages to {s}`Writer` producers
  *
  */
class BuilderExecutor[T: ClassTag](
    name: String,
    graphBuilder: GraphBuilder[T],
    conf: Config,
    pulsarController: PulsarController
) extends Component[T](conf, pulsarController) {
  private val safegraphBuilder     = Marshal.deepCopy(graphBuilder)
  private val producers            = pulsarController.toWriterProducers
  private val failOnError: Boolean = conf.getBoolean("raphtory.builders.failOnError")

  private var messagesProcessed = 0

  var cancelableConsumer: Option[Consumer[Array[Byte]]] = None

  override def run(): Unit = {
    logger.debug(
            s"Starting Graph Builder executor with deploymentID ${conf.getString("raphtory.deploy.id")}"
    )

    cancelableConsumer = Some(
            pulsarController.startGraphBuilderConsumer(messageListener())
    )

  }

  override def stop(): Unit = {
    logger.debug("Stopping Graph Builder executor.")

    cancelableConsumer match {
      case Some(value) =>
        value.close()
      case None        =>
    }

    producers.foreach(_._2.close())
  }

  override def handleMessage(msg: T): Unit =
    safegraphBuilder
      .getUpdates(msg)(failOnError = failOnError)
      .foreach(message => sendUpdate(message))

  protected def sendUpdate(graphUpdate: GraphUpdate): Unit = {
    logger.trace(s"Sending graph update: $graphUpdate")

    producers(getWriter(graphUpdate.srcId))
      .sendAsync(serialise(graphUpdate))
      .thenApply(msgId => msgId -> null)

    messagesProcessed = messagesProcessed + 1

    if (messagesProcessed % 100_000 == 0)
      logger.debug(s"Graph builder $name: sent $messagesProcessed messages.")
  }
}
