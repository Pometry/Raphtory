package com.raphtory.internals.management

import cats.effect.IO
import cats.effect.Resource
import cats.effect.Sync
import org.apache.curator.framework._
import org.apache.curator.retry.ExponentialBackoffRetry
import scala.collection.immutable.HashMap

trait ZookeeperConnector {

  private val zkClients = HashMap[String, Resource[IO, CuratorFramework]]()

  def getZkClient(zkAddress: String): Resource[IO, CuratorFramework] =
    if (zkClients.isDefinedAt(zkAddress))
      zkClients(zkAddress)
    else
      Resource
        .fromAutoCloseable(
                Sync[IO]
                  .delay {
                    CuratorFrameworkFactory
                      .builder()
                      .connectString(zkAddress)
                      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                      .build()
                  }
                  .flatTap(c => Sync[IO].blocking(c.start()))
        )
}
