package com.raphtory.internals.management.id

import cats.effect.Resource
import cats.effect.Sync
import com.typesafe.scalalogging.Logger
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.retry.RetryNTimes
import org.slf4j.LoggerFactory;
import cats.effect.Resource
import cats.effect.Sync
import cats.syntax.all._

private[raphtory] class ZooKeeperCounter(
    zookeeperAddress: String,
    deploymentID: String,
    poolID: String,
    client: CuratorFramework
) extends IDManager {
  val logger: Logger    = Logger(LoggerFactory.getLogger(this.getClass))
  private val idSetPath = s"/$deploymentID/$poolID"

  private val atomicInt: DistributedAtomicInteger =
    new DistributedAtomicInteger(client, idSetPath, new RetryNTimes(10, 500), null);

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

}

private[raphtory] object ZooKeeperCounter {

  def apply[IO[_]: Sync](
      zookeeperAddress: String,
      graphId: String,
      poolID: String
  ): Resource[IO, ZooKeeperCounter] =
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
      .map(new ZooKeeperCounter(zookeeperAddress, graphId, poolID, _))
}
