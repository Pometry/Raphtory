package com.raphtory

import java.lang.management.ManagementFactory
import java.net.InetAddress

import ch.qos.logback.classic.Level
import com.raphtory.core.analysis.Tasks.AnalysisTask
import com.raphtory.core.clustersetup._
import com.raphtory.core.clustersetup.singlenode.SingleNodeSetup
import com.raphtory.examples.random.actors.RandomRouter
import com.raphtory.examples.random.actors.RandomSpout
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import kamon.metric.PeriodSnapshot
import kamon.prometheus.PrometheusReporter
import kamon.system.SystemMetrics
import kamon.Kamon
import kamon.MetricReporter
import org.slf4j.LoggerFactory

import scala.language.postfixOps
import scala.sys.process._
//main function

object Go extends App {

  val conf    = ConfigFactory.load()
  val seedLoc = s"${sys.env("HOST_IP")}:${conf.getInt("settings.bport")}"
  val root    = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger]
  root.setLevel(Level.ERROR)
  val routerName  = s"${sys.env.getOrElse("ROUTERCLASS", classOf[RandomRouter].getClass.getName)}"
  val updaterName = s"${sys.env.getOrElse("UPDATERCLASS", classOf[RandomSpout].getClass.getName)}"
  val lamName     = s"${sys.env.getOrElse("LAMCLASS", classOf[AnalysisTask].getClass.getName)}"
  val docker      = System.getenv().getOrDefault("DOCKER", "false").trim.toBoolean

  val runtimeMxBean = ManagementFactory.getRuntimeMXBean
  val arguments     = runtimeMxBean.getInputArguments

  println(s"Current java options: $arguments")
  args(0) match {
    case "seedNode" =>
      println("Creating seed node")
      setConf(seedLoc)
      SeedNode(seedLoc)
    case "router" =>
      println("Creating Router")
      RouterNode(getConf(), sys.env("PARTITION_MIN").toInt, routerName)
    case "partitionManager" =>
      println(s"Creating Partition Manager...")
      ManagerNode(getConf(), sys.env("PARTITION_MIN").toInt)

    case "updater" =>
      println("Creating Update Generator")
      UpdateNode(getConf(), updaterName)

    case "liveAnalysis" =>
      println("Creating Analysis Manager")
      LiveAnalysisNode(getConf(), lamName)
    case "clusterUp" =>
      println("Cluster Up, informing Partition Managers and Routers")
      WatchDogNode(getConf(), sys.env("PARTITION_MIN").toInt, sys.env("ROUTER_MIN").toInt)

    case "singleNodeSetup" =>
      println("putting up cluster in one node")
      SingleNodeSetup(
              hostname2Ip(seedLoc),
              routerName,
              updaterName,
              lamName,
              sys.env("PARTITION_MIN").toInt,
              sys.env("ROUTER_MIN").toInt
      )
      prometheusReporter()
  }

  def setConf(seedLoc: String): Unit = {
    println(s"I AM AT $seedLoc")
    prometheusReporter()
  }

  def getConf(): String =
    if (docker) {
      while (!("nc seedNode 1600" !).equals(0)) {
        println("Waiting for seednode to come online")
        Thread.sleep(3000)
      }
      prometheusReporter()
      hostname2Ip("seedNode:1600")
    } else "127.0.0.1"

  def prometheusReporter() = {
    try SystemMetrics.startCollecting()
    catch {
      case e: Exception => println("Error in pro")
    }
    val prom = new PrometheusReporter()

    val testLogger = new MetricReporter {

      override def reportPeriodSnapshot(snapshot: PeriodSnapshot): Unit =
        try prom.reportPeriodSnapshot(snapshot)
        catch {
          case e: Exception =>
            println(e)
            println("Hello I have broken and I cannot get up")
        }

      override def start(): Unit = prom.start()

      override def reconfigure(config: Config): Unit = prom.reconfigure(config)

      override def stop(): Unit = prom.stop()
    }
    Kamon.addReporter(testLogger)
  }

  def hostname2Ip(seedLoc: String): String = {
    // hostname_asd_1:port
    val t = seedLoc.split(":")
    InetAddress.getByName(t(0)).getHostAddress() + ":" + t(1)
  }
}
