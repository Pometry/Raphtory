package com.raphtory.components

import com.raphtory.config.telemetry.ComponentTelemetryHandler
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

/** @DoNotDocument */
abstract class Component[T](conf: Config) {

  val logger: Logger           = Logger(LoggerFactory.getLogger(this.getClass))
  val telemetry                = ComponentTelemetryHandler
  val partitionServers: Int    = conf.getInt("raphtory.partitions.serverCount")
  val partitionsPerServer: Int = conf.getInt("raphtory.partitions.countPerServer")
  val totalPartitions: Int     = partitionServers * partitionsPerServer
  val deploymentID: String     = conf.getString("raphtory.deploy.id")

  def getWriter(srcId: Long): Int = (srcId.abs % totalPartitions).toInt
  def handleMessage(msg: T): Unit
  def run(): Unit
  def stop(): Unit = {}
}
