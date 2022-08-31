package com.raphtory.internals.management.id

import cats.effect.Resource
import cats.effect.Sync
import cats.syntax.all._
import com.raphtory.internals.management.ZookeeperConnector

import com.typesafe.scalalogging.Logger
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode
import org.slf4j.LoggerFactory

import scala.util.Failure
import scala.util.Success
import scala.util.Try

private[raphtory] class ZookeeperLimitedPool(
    zookeeperAddress: String,
    deploymentID: String,
    poolID: String,
    poolSize: Int,
    client: CuratorFramework
) extends IDManager {
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))
  private val idSetPath      = s"/$deploymentID/$poolID"

  def getNextAvailableID(): Option[Int] = {
    val candidateIds = LazyList.from(0 until poolSize)

    val attempts       = candidateIds.map(id => allocateId(client, idSetPath, id))
    val failedAttempts = attempts.takeWhile(_.isInstanceOf[Failure[Int]])

    if (failedAttempts.size == poolSize) {
      logger.error(
              s"Zookeeper ($zookeeperAddress): Failed to get id after $poolSize attempts. Causes: $failedAttempts."
      )
      None
    }
    else {
      val firstSuccess = failedAttempts.size
      Some(attempts(firstSuccess).get)
    }
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

private[raphtory] object ZookeeperLimitedPool extends ZookeeperConnector {

  def apply[IO[_]: Sync](
      zookeeperAddress: String,
      graphId: String,
      poolID: String,
      poolSize: Int
  ): Resource[IO, ZookeeperLimitedPool] =
    ZookeeperConnector
      .getZkClient(zookeeperAddress)
      .asInstanceOf[Resource[IO, CuratorFramework]]
      .map(new ZookeeperLimitedPool(zookeeperAddress, graphId, poolID, poolSize, _))

  def apply[IO[_]: Sync](
      zookeeperAddress: String,
      deploymentId: String,
      counterId: String
  ): Resource[IO, ZookeeperLimitedPool] =
    apply(zookeeperAddress, deploymentId, counterId, Int.MaxValue)
}
