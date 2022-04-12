package com.raphtory.components.graphbuilder

import com.raphtory.components.Component
import com.raphtory.config.Gateway
import com.raphtory.config.PulsarController
import com.raphtory.serialisers.Marshal
import com.typesafe.config.Config
import org.apache.pulsar.client.admin.PulsarAdminException
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.Schema

import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

/** @DoNotDocument */
class BuilderExecutor[T: ClassTag](
    name: String,
    graphBuilder: GraphBuilder[T],
    conf: Config,
    gateway: Gateway
) extends Component[T](conf, gateway) {
  private val safegraphBuilder     = Marshal.deepCopy(graphBuilder)
  private val failOnError: Boolean = conf.getBoolean("raphtory.builders.failOnError")
  private val writers              = gateway.toGraphUpdates(update => getWriter(update.srcId))

  private var messagesProcessed = 0

  override def setup(): Unit =
    logger.debug(
            s"Starting Graph Builder executor with deploymentID ${conf.getString("raphtory.deploy.id")}"
    )

  override def stopHandler(): Unit = {
    logger.debug("Stopping Graph Builder executor.")
    writers.close()
  }

  override def handleMessage(msg: T): Unit =
    safegraphBuilder
      .getUpdates(msg)(failOnError = failOnError)
      .foreach(message => sendUpdate(message))

  protected def sendUpdate(graphUpdate: GraphUpdate): Unit = {
    logger.trace(s"Sending graph update: $graphUpdate")

    writers.sendAsync(graphUpdate)
    //.thenApply(msgId => msgId -> null) TODO: remove I guess ?

    messagesProcessed = messagesProcessed + 1

    if (messagesProcessed % 100_000 == 0)
      logger.debug(s"Graph builder $name: sent $messagesProcessed messages.")
  }
}
