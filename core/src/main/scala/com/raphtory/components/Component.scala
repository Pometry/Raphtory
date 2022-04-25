package com.raphtory.components

import com.raphtory.config.Gateway
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

/** @DoNotDocument */
abstract class Component[T](conf: Config, private val gateway: Gateway) {

  class TaskList() {
    var tasks: Seq[() => Unit]      = Seq()
    def add(task: () => Unit): Unit = tasks = tasks.appended(task)
    def run(): Unit                 = tasks.foreach(task => task())
  }

  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  // TODO: remove this from here
  val partitionServers: Int    = conf.getInt("raphtory.partitions.serverCount")
  val partitionsPerServer: Int = conf.getInt("raphtory.partitions.countPerServer")
  val totalPartitions: Int     = partitionServers * partitionsPerServer

  val listeningSetup: TaskList   = new TaskList()
  val listeningCleanup: TaskList = new TaskList()

  def stopHandler(): Unit = {}
  def setup(): Unit = {}

  final def run(): Unit = {
    setup()
    listeningSetup.run()
  }

  final def stop(): Unit = {
    stopHandler()
    listeningCleanup.run()
  }

  protected def getWriter(srcId: Long): Int = (srcId.abs % totalPartitions).toInt
}
