package com.raphtory.core.components

import com.raphtory.core.components.graphbuilder.GraphAlteration
import com.raphtory.core.config.PulsarController
import com.raphtory.serialisers.PulsarKryoSerialiser
import com.raphtory.serialisers.avro
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.apache.pulsar.client.admin.PulsarAdminException
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageListener
import org.apache.pulsar.client.api.Producer
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.common.policies.data.RetentionPolicies
import org.slf4j.LoggerFactory

import scala.reflect.runtime.universe._

/** @DoNotDocument */
abstract class Component[T: TypeTag](conf: Config, private val pulsarController: PulsarController)
        extends Runnable {

  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  val pulsarAddress: String      = conf.getString("raphtory.pulsar.broker.address")
  val pulsarAdminAddress: String = conf.getString("raphtory.pulsar.admin.address")
  val spoutTopic: String         = conf.getString("raphtory.spout.topic")
  val deploymentID: String       = conf.getString("raphtory.deploy.id")
  val partitionServers: Int      = conf.getInt("raphtory.partitions.serverCount")
  val partitionsPerServer: Int   = conf.getInt("raphtory.partitions.countPerServer")
  val hasDeletions: Boolean      = conf.getBoolean("raphtory.data.containsDeletions")
  val totalPartitions: Int       = partitionServers * partitionsPerServer
  val kryo: PulsarKryoSerialiser = PulsarKryoSerialiser()

  def handleMessage(msg: T)
  def run()
  def stop()

  def messageListener(): MessageListener[Array[Byte]] =
    (consumer, msg) => {
      try {
        val data = deserialise[T](msg.getValue)

        handleMessage(data)

        consumer.acknowledgeAsync(msg)
      }
      catch {
        case e: Exception =>
          logger.error(s"Deployment $deploymentID: Failed to handle message.")
          consumer.negativeAcknowledge(msg)
          throw e
      }
      finally msg.release()
    }

  def serialise(value: Any): Array[Byte] = kryo.serialise(value)

  def deserialise[T: TypeTag](bytes: Array[Byte]): T = kryo.deserialise[T](bytes)

  def getWriter(srcId: Long): Int = (srcId.abs % totalPartitions).toInt

}
