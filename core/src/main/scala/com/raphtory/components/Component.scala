package com.raphtory.components

import com.raphtory.config.telemetry.ComponentTelemetryHandler
import com.typesafe.config.Config

/** @note DoNotDocument */
abstract class Component[T](conf: Config) {

  protected val telemetry: ComponentTelemetryHandler.type = ComponentTelemetryHandler
  private val partitionServers: Int                       = conf.getInt("raphtory.partitions.serverCount")
  private val partitionsPerServer: Int                    = conf.getInt("raphtory.partitions.countPerServer")
  protected val totalPartitions: Int                      = partitionServers * partitionsPerServer
  protected val deploymentID: String                      = conf.getString("raphtory.deploy.id")

  def getWriter(srcId: Long): Int = (srcId.abs % totalPartitions).toInt
  def handleMessage(msg: T): Unit
  def run(): Unit
  def stop(): Unit = {}
}
