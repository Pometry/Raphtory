package com.raphtory.internals.management.id

import com.typesafe.scalalogging.Logger
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._

private[raphtory] class ZookeeperIDManager(
    zookeeperAddress: String,
    deploymentID: String,
    counterID: String
) extends IDManager {
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private val parentPath   = s"/$deploymentID/$counterID"
  private val sequencePath = s"$parentPath/id"

  private val client = CuratorFrameworkFactory
    .builder()
    .connectString(zookeeperAddress)
    .retryPolicy(new ExponentialBackoffRetry(1000, 3))
    .build();

  client.start()

  def getNextAvailableID(): Option[Int] =
    try {
      val sequenceNumber = client
        .create()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
        .forPath(sequencePath)
        .split("/")
        .last
      val id             = client.getChildren.forPath(parentPath).asScala.sorted.indexOf(sequenceNumber)
      logger.trace(s"Zookeeper $zookeeperAddress: Get new id '$id'.")
      Some(id)
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
        logger.error(s"Zookeeper $zookeeperAddress: Failed to get id.")
        None
    }

  def stop(): Unit = client.close()
}
