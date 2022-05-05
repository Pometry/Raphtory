package com.raphtory.config

import com.typesafe.scalalogging.Logger
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.retry.RetryNTimes
import org.slf4j.LoggerFactory;

/** @note DoNotDocument */
private[raphtory] class ZookeeperIDManager(zookeeperAddress: String, atomicPath: String) {
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private val client: CuratorFramework = CuratorFrameworkFactory
    .builder()
    .connectString(zookeeperAddress)
    .retryPolicy(new ExponentialBackoffRetry(1000, 3))
    .build();

  private val atomicInt: DistributedAtomicInteger =
    new DistributedAtomicInteger(client, atomicPath, new RetryNTimes(10, 500), null);

  client.start

  def getNextAvailableID(): Option[Int] = {
    val incremented = atomicInt.increment()

    if (incremented.succeeded()) {
      val id = incremented.preValue()

      logger.trace(s"Zookeeper $zookeeperAddress: Atomic integer pre value at '$id'.")

      Some(id)
    }
    else {
      logger.error(s"Zookeeper $zookeeperAddress: Failed to increment atomic integer.")

      None
    }
  }

  def resetID(): Unit = {
    logger.debug(s"Zookeeper $zookeeperAddress: Atomic integer requested for reset.")

    logger.debug(
            s"Zookeeper $zookeeperAddress: Atomic integer value now at '${atomicInt.get().preValue()}'."
    )

    atomicInt.forceSet(0)

    logger.debug(
            s"Zookeeper $zookeeperAddress: Atomic integer value now at '${atomicInt.get().postValue()}'."
    )
  }

  def stop(): Unit =
    client.close()

}
