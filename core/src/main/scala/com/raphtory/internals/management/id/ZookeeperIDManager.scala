package com.raphtory.internals.management.id

import cats.effect.Resource
import cats.effect.Sync
import cats.syntax.all._
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
    poolSize: Int,
    client: CuratorFramework
) extends IDManager {
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))
  private val idSetPath      = s"/$deploymentID/$poolID"

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
      deploymentId: String,
      poolID: String,
      poolSize: Int
  ): Resource[IO, ZookeeperIDManager] =
    Resource
      .fromAutoCloseable(
              Sync[IO]
                .delay {
                  CuratorFrameworkFactory
                    .builder()
                    .connectString(zookeeperAddress)
                    .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                    .build();
                }
                .flatTap(c => Sync[IO].blocking(c.start()))
      )
      .map(new ZookeeperIDManager(zookeeperAddress, deploymentId, poolID, poolSize, _))

  def apply[IO[_]: Sync](
      zookeeperAddress: String,
      deploymentId: String,
      counterId: String
  ): Resource[IO, ZookeeperIDManager] =
    apply(zookeeperAddress, deploymentId, counterId, Int.MaxValue)
}
