package com.raphtory.internals.management

import cats.effect.IO
import cats.effect.Resource
import cats.effect.Sync
import org.apache.curator.framework._
import org.apache.curator.retry.ExponentialBackoffRetry
import scala.collection.immutable.HashMap

trait ZookeeperConnector {}

object ZookeeperConnector {

  def getZkClient(zkAddress: String): Resource[IO, CuratorFramework] =
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
