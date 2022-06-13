package com.raphtory.internals.management.id

import cats.effect.Resource
import cats.effect.Sync
import com.typesafe.scalalogging.Logger
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode
import org.slf4j.LoggerFactory

import scala.util.Success
import scala.util.Try

private[raphtory] class ZookeeperIDManager(
    zookeeperAddress: String,
    deploymentID: String,
    poolID: String,
    poolSize: Int
) extends IDManager {
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))
  private val idSetPath      = s"/$deploymentID/$poolID"

  private val client = CuratorFrameworkFactory
    .builder()
    .connectString(zookeeperAddress)
    .retryPolicy(new ExponentialBackoffRetry(1000, 3))
    .build();

  client.start()

  def getNextAvailableID(): Option[Int] = {
    val candidateIds = (0 until poolSize).iterator

    val id = candidateIds
      .map(id => allocateId(client, idSetPath, id))
      .collect { case Success(id) => id }
      .nextOption()

    id match {
      case Some(id) => logger.trace(s"Zookeeper $zookeeperAddress: Get new id '$id'.")
      case None     => logger.error(s"Zookeeper $zookeeperAddress: Failed to get id.")
    }
    id
  }

  def stop(): Unit = client.close()

  private def allocateId(client: CuratorFramework, idSetPath: String, id: Int): Try[Int] =
    Try {
      client
        .create()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.EPHEMERAL)
        .forPath(s"$idSetPath/$id/allocated")
      id
    }
}

private[raphtory] object ZookeeperIDManager {

  def apply(
      zookeeperAddress: String,
      deploymentID: String,
      counterID: String
  ) = new ZookeeperIDManager(zookeeperAddress, deploymentID, counterID, Int.MaxValue)

  def apply(
      zookeeperAddress: String,
      deploymentID: String,
      poolID: String,
      poolSize: Int
  ) = new ZookeeperIDManager(zookeeperAddress, deploymentID, poolID, poolSize)

  def apply[IO[_]: Sync](
                          zookeeperAddress: String,
                          atomicPath: String
                        ): Resource[IO, ZookeeperIDManager] =
    Resource
      .fromAutoCloseable(Sync[IO].delay {
        CuratorFrameworkFactory
          .builder()
          .connectString(zookeeperAddress)
          .retryPolicy(new ExponentialBackoffRetry(1000, 3))
          .build();
      })
      .map(new ZookeeperIDManager(zookeeperAddress, atomicPath, _))

}
