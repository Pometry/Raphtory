package com.raphtory.components

import com.raphtory.config.PulsarController
import com.raphtory.config.telemetry.ComponentTelemetryHandler
import com.raphtory.serialisers.PulsarKryoSerialiser
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.api.MessageListener
import org.slf4j.LoggerFactory

import scala.math.Ordering.Implicits.infixOrderingOps
import scala.reflect.runtime.universe._

/** @DoNotDocument */
abstract class Component[T](conf: Config, private val pulsarController: PulsarController)
        extends Runnable {

  val logger: Logger             = Logger(LoggerFactory.getLogger(this.getClass))
  val telemetry                  = ComponentTelemetryHandler
  val pulsarAddress: String      = conf.getString("raphtory.pulsar.broker.address")
  val pulsarAdminAddress: String = conf.getString("raphtory.pulsar.admin.address")
  val spoutTopic: String         = conf.getString("raphtory.spout.topic")
  val deploymentID: String       = conf.getString("raphtory.deploy.id")
  val partitionServers: Int      = conf.getInt("raphtory.partitions.serverCount")
  val partitionsPerServer: Int   = conf.getInt("raphtory.partitions.countPerServer")
  val hasDeletions: Boolean      = conf.getBoolean("raphtory.data.containsDeletions")
  val totalPartitions: Int       = partitionServers * partitionsPerServer
  val kryo: PulsarKryoSerialiser = PulsarKryoSerialiser()
  var lastId: MessageId          = MessageId.earliest

  def handleMessage(msg: T)
  def run()
  def stop()

  def messageListener(): MessageListener[Array[Byte]] =
    (consumer, msg) => {
      try {
        val data = deserialise[T](msg.getValue)
        val id   = msg.getMessageId
        if (id <= lastId)
          logger.error(
                  s"Message $data received with index $id <= last index $lastId. Ignoring message"
          )
        else {
          lastId = id

          handleMessage(data)
        }
        consumer.acknowledgeAsync(msg)
      }
      catch {
        case e: Exception =>
          e.printStackTrace()
          logger.error(s"Deployment $deploymentID: Failed to handle message. ${e.getMessage}")
          consumer.negativeAcknowledge(msg)
          throw e
      }
      finally msg.release()
    }

  def serialise(value: Any): Array[Byte] = kryo.serialise(value)

  def deserialise[T](bytes: Array[Byte]): T = kryo.deserialise[T](bytes)

  def getWriter(srcId: Long): Int = (srcId.abs % totalPartitions).toInt

}
