package com.raphtory.core.components.graphbuilder

import com.raphtory.core.components.Component
import com.raphtory.core.config.PulsarController
import com.rits.cloning.Cloner
import com.typesafe.config.Config
import org.apache.pulsar.client.admin.PulsarAdminException
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.Schema

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

class BuilderExecutor[T](
    schema: Schema[T],
    graphBuilder: GraphBuilder[T],
    conf: Config,
    pulsarController: PulsarController
) extends Component[T](conf, pulsarController) {
  private val safegraphBuilder = new Cloner().deepClone(graphBuilder)
  private val producers        = toWriterProducers

  var cancelableConsumer: Option[Consumer[T]] = None
  setupNamespace()

  override def run(): Unit = {
    logger.debug("Starting Graph Builder executor.")

    cancelableConsumer = Some(startGraphBuilderConsumer(schema))

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

  def setupNamespace(): Unit =
    try pulsarController.pulsarAdmin.namespaces().createNamespace("public/raphtory_builder_exec")
    catch {
      case error: PulsarAdminException =>
        logger.warn("Namespace already found")
    }
    finally pulsarController.setRetentionNamespace("public/raphtory_builder_exec")

  override def handleMessage(msg: Message[T]): Unit = {
    val data = msg.getValue
    safegraphBuilder.getUpdates(data).foreach(message => sendUpdate(message))
  }

  protected def sendUpdate(graphUpdate: GraphUpdate): Unit = {
    logger.trace(s"Sending graph update: $graphUpdate")
    producers(getWriter(graphUpdate.srcId)).sendAsync(graphUpdate)

  }
}
