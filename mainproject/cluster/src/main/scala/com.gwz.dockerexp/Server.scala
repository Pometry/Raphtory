package com.gwz.dockerexp

import java.net.InetAddress

import com.gwz.dockerexp.caseclass.clustercase._
import kamon.Kamon
import kamon.prometheus.PrometheusReporter
import kamon.system.SystemMetrics
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.slf4j.LoggerFactory;

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
      RestNode(getConf(args(1)))
    }
    case "router" => {
      println("Creating Router")
      RouterNode(getConf(args(2)), args(1))
    }
    case "partitionManager" => {
      println(s"Creating Patition Manager ID: ${args(1)}")
      ManagerNode(getConf(args(3)), args(1), args(2))
    }

    case "updateGen" => {
      println("Creating Update Generator")
      UpdateNode(getConf(args(2)), args(1))
    }

    case "benchmark" => {
      println("Creating benchmarker")
      BenchmarkNode(getConf(args(2)), args(1))
    }
    case "LiveAnalysisManager" => {
      println("Creating Live Analysis Manager")
      LiveAnalysisNode(getConf(args(2)), args(1),args(3))
    }
    case "ClusterUp" => {
      println("Cluster Up, informing Partition Managers and Routers")
      ClusterUpNode(getConf(args(2)), args(1))
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
