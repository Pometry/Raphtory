package com.raphtory

import java.net.InetAddress

import ch.qos.logback.classic.Level
import com.raphtory.core.clustersetup._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.slf4j.LoggerFactory
import kamon.{Kamon, MetricReporter}
import kamon.prometheus.PrometheusReporter
import kamon.system.SystemMetrics
import java.lang.management.ManagementFactory

import akka.http.scaladsl.server.ExceptionHandler
import com.raphtory.core.actors.analysismanager.LiveAnalysisManager
import com.raphtory.core.clustersetup._
import com.raphtory.core.clustersetup.singlenode.SingleNodeSetup
import com.raphtory.examples.random.actors.{RandomRouter, RandomSpout}
import kamon.metric.PeriodSnapshot

import scala.language.postfixOps
import scala.sys.process._
//main function



object Go extends App {

  val conf          = ConfigFactory.load()
  val seedLoc       = s"${sys.env("HOST_IP")}:${conf.getInt("settings.bport")}"
 // val zookeeper     = s"${sys.env("ZOOKEEPER")}"
 val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger]
  root.setLevel(Level.ERROR)
  val routerName    = s"${sys.env.getOrElse("ROUTERCLASS", classOf[RandomRouter].getClass.getName)}"
  val updaterName   = s"${sys.env.getOrElse("UPDATERCLASS", classOf[RandomSpout].getClass.getName)}"
  val lamName       = s"${sys.env.getOrElse("LAMCLASS", classOf[LiveAnalysisManager].getClass.getName)}"

  val runtimeMxBean = ManagementFactory.getRuntimeMXBean
  val arguments = runtimeMxBean.getInputArguments

  println(s"Current java options: ${arguments}")
  args(0) match {
    case "seedNode" => {
      println("Creating seed node")
      setConf(seedLoc)
      SeedNode(seedLoc)
    }
    case "rest" => {
      println("Creating rest node")
      RestNode(getConf())
    }
    case "router" => {
      println("Creating Router")
      RouterNode(getConf(), sys.env("PARTITION_MIN").toInt, routerName)
    }
    case "partitionManager" => {
      println(s"Creating Patition Manager...")
      ManagerNode(getConf(), sys.env("PARTITION_MIN").toInt)
    }

    case "updater" => {
      println("Creating Update Generator")
      UpdateNode(getConf(), updaterName)
    }

    case "liveAnalysis" => {
      println("Creating Live Analysis Manager")
      //val LAM_Name = args(1) // TODO other ways (still env?): see issue #5 #6

      LiveAnalysisNode(getConf(), lamName)
    }
    case "clusterUp" => {
      println("Cluster Up, informing Partition Managers and Routers")
      WatchDogNode(getConf(), sys.env("PARTITION_MIN").toInt, sys.env("ROUTER_MIN").toInt)
    }

    case "singleNodeSetup" => {
      println("putting up cluster in one node")
      //setConf(seedLoc, zookeeper)
      SingleNodeSetup(hostname2Ip(seedLoc), routerName, updaterName, lamName, sys.env("PARTITION_MIN").toInt, sys.env("ROUTER_MIN").toInt)
      prometheusReporter()
    }
  }

  def setConf(seedLoc: String): Unit ={
    println(s"I AM AT $seedLoc")
    prometheusReporter()
  }

  def getConf():String = {
    while(!("nc seedNode 1600" !).equals(0)){
      println("Waiting for seednode to come online")
      Thread.sleep(3000)
    }
    prometheusReporter()
    hostname2Ip("seedNode:1600")

  }
  //https://blog.knoldus.com/2014/08/29/how-to-setup-and-use-zookeeper-in-scala-using-apache-curator/

  def prometheusReporter() = {
    try {
      //SystemMetrics.startCollecting()
    }catch {
      case e:Exception => println("Error in pro")
    }
    val prom = new PrometheusReporter()


    val testLogger = new MetricReporter {

      override def reportPeriodSnapshot(snapshot: PeriodSnapshot): Unit = {
        try {
          prom.reportPeriodSnapshot(snapshot)
        }catch {
          case e:Exception => {
            println(e)
            println("Hello I have broken and I cannot get up")
          }
        }
      }

      override def start(): Unit = prom.start()

      override def reconfigure(config: Config): Unit = prom.reconfigure(config)

      override def stop(): Unit = prom.stop()
    }
    //Kamon.addReporter(testLogger)
  }

  def hostname2Ip(seedLoc: String): String = {
    // hostname_asd_1:port
    val t = seedLoc.split(":")
    return InetAddress.getByName(t(0)).getHostAddress() + ":" + t(1)
  }
}
