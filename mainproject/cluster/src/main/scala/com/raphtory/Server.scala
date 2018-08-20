package com.raphtory

import java.net.InetAddress

import ch.qos.logback.classic.Level
import com.raphtory.core.clustersetup._
import com.typesafe.config.ConfigFactory
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.slf4j.LoggerFactory
import kamon.Kamon
import kamon.prometheus.PrometheusReporter
import kamon.system.SystemMetrics
import java.lang.management.ManagementFactory

import com.raphtory.core.actors.analysismanager.LiveAnalysisManager
import com.raphtory.core.clustersetup._
import com.raphtory.core.clustersetup.singlenode.SingleNodeSetup
import com.raphtory.examples.random.actors.{RandomRouter, RandomSpout}
import scala.language.postfixOps
import scala.sys.process._
//main function
object Go extends App {
  val conf          = ConfigFactory.load()
  val seedLoc       = s"${sys.env("HOST_IP")}:${conf.getInt("settings.bport")}"
  val zookeeper     = s"${sys.env("ZOOKEEPER")}"

  val routerName    = s"${sys.env.getOrElse("ROUTERCLASS", classOf[RandomRouter].getClass.getName)}"
  val updaterName   = s"${sys.env.getOrElse("UPDATERCLASS", classOf[RandomSpout].getClass.getName)}"
  val lamName       = s"${sys.env.getOrElse("LAMCLASS", classOf[LiveAnalysisManager].getClass.getName)}"

  val runtimeMxBean = ManagementFactory.getRuntimeMXBean
  val arguments = runtimeMxBean.getInputArguments

  println(s"Current java options: ${arguments}")

  args(0) match {
    case "seedNode" => {
      println("Creating seed node")
      setConf(seedLoc, zookeeper)
      SeedNode(seedLoc)
    }
    case "rest" => {
      println("Creating rest node")
      RestNode(getConf(zookeeper))
    }
    case "router" => {
      println("Creating Router")
      RouterNode(getConf(zookeeper), routerName)
    }
    case "partitionManager" => {
      println(s"Creating Patition Manager...")
      ManagerNode(getConf(zookeeper))
    }

    case "updater" => {
      println("Creating Update Generator")
      UpdateNode(getConf(zookeeper), updaterName)
    }

    case "liveAnalysis" => {
      println("Creating Live Analysis Manager")
      //val LAM_Name = args(1) // TODO other ways (still env?): see issue #5 #6

      LiveAnalysisNode(getConf(zookeeper), lamName)
    }
    case "clusterUp" => {
      println("Cluster Up, informing Partition Managers and Routers")
      WatchDogNode(getConf(zookeeper), sys.env("PARTITION_MIN").toInt, sys.env("ROUTER_MIN").toInt)
    }

    case "singleNodeSetup" => {
      println("putting up cluster in one node")
      //setConf(seedLoc, zookeeper)
      SingleNodeSetup(hostname2Ip(seedLoc),routerName,updaterName,lamName,sys.env("PARTITION_MIN").toInt, sys.env("ROUTER_MIN").toInt)
    }
  }

  def setConf(seedLoc: String, zookeeper: String): Unit ={
    println(s"I AM AT $seedLoc")
    prometheusReporter()
  }

//  def setConf(seedLoc: String, zookeeper: String): Unit ={
//    var zookeeperFailed = true
//    while(zookeeperFailed){
//      try {
//        zookeeperFailed = setConfHelper(seedLoc, zookeeper)
//      }
//      catch {
//        case e:Exception => println("Zookeeper Timeout")
//      }
//    }
//  }

  def setConfHelper(seedLoc: String, zookeeper: String): Boolean = {
    try {
      val retryPolicy = new ExponentialBackoffRetry(1000, 3)
      val curatorZookeeperClient =
        CuratorFrameworkFactory.newClient(zookeeper, retryPolicy)

      val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger]
      root.setLevel(Level.ERROR)


      curatorZookeeperClient.start
      curatorZookeeperClient.getZookeeperClient.blockUntilConnectedOrTimedOut
      if (curatorZookeeperClient.checkExists().forPath("/seednode") == null) {
        curatorZookeeperClient
          .create()
          .creatingParentsIfNeeded()
          .forPath("/seednode", seedLoc.getBytes)
      } else {
        curatorZookeeperClient.setData().forPath("/seednode", seedLoc.getBytes)
      }
      val originalData = new String(
        curatorZookeeperClient.getData.forPath("/seednode"))
      curatorZookeeperClient.close()
      false
    }
    catch {
      case e:Exception => {
        println("zookeeper currently unavailable, trying again in 10 seconds")
        true
      }
    }
  }

  def getConf(zookeeper:String):String = {
    while(!("nc seedNode 1600" !).equals(0)){
      println("Waiting for seednode to come online")
      Thread.sleep(3000)
    }
    prometheusReporter()
    hostname2Ip("seedNode:1600")

  }

//  def getConf(zookeeper: String): String = {
//    var zookeeperFailed = true
//    var seedlocation = ""
//    while(zookeeperFailed){
//      try {
//        val pair = getConfHelper(zookeeper)
//        zookeeperFailed = pair._2
//        seedlocation = pair._1
//      }
//      catch {
//        case e:Exception => println("Zookeeper Timeout")
//      }
//    }
//    seedlocation
//  }

  def getConfHelper(zookeeper: String): (String,Boolean) = {
    try {
      val retryPolicy = new ExponentialBackoffRetry(1000, 3)
      val curatorZookeeperClient =
        CuratorFrameworkFactory.newClient(zookeeper, retryPolicy)

      val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger]
      root.setLevel(Level.ERROR)

      curatorZookeeperClient.start
      curatorZookeeperClient.getZookeeperClient.blockUntilConnectedOrTimedOut
      val originalData = new String(
        curatorZookeeperClient.getData.forPath("/seednode"))
      println(originalData)
      prometheusReporter()
      curatorZookeeperClient.close()
      (hostname2Ip(originalData),false)
    }
    catch {
      case e:Exception =>{
        println("zookeeper currently unavailable, trying again in 10 seconds")
        ("",true)
      }

    }
  }
  //https://blog.knoldus.com/2014/08/29/how-to-setup-and-use-zookeeper-in-scala-using-apache-curator/

  def prometheusReporter() = {
    SystemMetrics.startCollecting()
    Kamon.addReporter(new PrometheusReporter())
  }

  def hostname2Ip(seedLoc: String): String = {
    // hostname_asd_1:port
    val t = seedLoc.split(":")
    return InetAddress.getByName(t(0)).getHostAddress() + ":" + t(1)
  }
}
