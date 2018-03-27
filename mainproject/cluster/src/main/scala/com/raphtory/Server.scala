package com.raphtory

import java.net.InetAddress

import com.raphtory.caseclass.clustercase._
import com.raphtory.caseclass.clustercase.{WatchDogNode, ManagerNode, RestNode, SeedNode}
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.slf4j.LoggerFactory
import kamon.Kamon
import kamon.prometheus.PrometheusReporter
import kamon.system.SystemMetrics

//main function
object Go extends App {
  args(0) match {
    case "seed" => {
      println("Creating seed node")
      val seedloc = args(1)
      val zookeeper = args(2)
      setConf(seedloc, zookeeper)
      SeedNode(seedLoc2Ip(seedloc))
    }
    case "rest" => {
      println("Creating rest node")
      val zookeeper = args(1)
      RestNode(getConf(zookeeper))
    }
    case "router" => {
      println("Creating Router")
      val zookeeper = args(2)
      val partitionCount = args(1)
      RouterNode(getConf(zookeeper),partitionCount)
    }
    case "partitionManager" => {
      println(s"Creating Patition Manager ID: ${args(1)}")
      val zookeeper = args(3)
      val partitionCount = args(2)
      val partitionID = args(1)
      ManagerNode(getConf(zookeeper), partitionID, partitionCount)
    }

    case "updateGen" => {
      println("Creating Update Generator")
      val zookeeper = args(2)
      val partitionCount = args(1)
      UpdateNode(getConf(zookeeper), partitionCount)
    }

    case "LiveAnalysisManager" => {
      println("Creating Live Analysis Manager")
      val zookeeper = args(2)
      val partitionCount = args(1)
      val LAM_Name = args(3)
      LiveAnalysisNode(getConf(zookeeper), partitionCount, LAM_Name)
    }
    case "ClusterUp" => {
      println("Cluster Up, informing Partition Managers and Routers")
      val zookeeper = args(2)
      val partitionCount = args(1)
      WatchDogNode(getConf(zookeeper), partitionCount)
    }

  }
  def setConf(seedLoc: String, zookeeper: String): Unit = {
    val retryPolicy = new ExponentialBackoffRetry(1000, 3)
    val curatorZookeeperClient =
      CuratorFrameworkFactory.newClient(zookeeper, retryPolicy)
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
  }

  def getConf(zookeeper: String): String = {
    val retryPolicy = new ExponentialBackoffRetry(1000, 3)
    val curatorZookeeperClient =
      CuratorFrameworkFactory.newClient(zookeeper, retryPolicy)
    val logger = LoggerFactory.getLogger("Server");
    curatorZookeeperClient.start
    curatorZookeeperClient.getZookeeperClient.blockUntilConnectedOrTimedOut
    val originalData = new String(
      curatorZookeeperClient.getData.forPath("/seednode"))
    println(originalData)
    prometheusReporter()
    curatorZookeeperClient.close()
    seedLoc2Ip(originalData)
  }
  //https://blog.knoldus.com/2014/08/29/how-to-setup-and-use-zookeeper-in-scala-using-apache-curator/

  def prometheusReporter() = {
    SystemMetrics.startCollecting()
    Kamon.addReporter(new PrometheusReporter())
  }

  def seedLoc2Ip(seedLoc: String): String = {
    // hostname_asd_1:port
    val t = seedLoc.split(":")
    return InetAddress.getByName(t(0)).getHostAddress() + ":" + t(1)
  }

}
