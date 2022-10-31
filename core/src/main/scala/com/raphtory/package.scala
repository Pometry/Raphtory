package com

import cats.effect.Resource
import cats.effect.Sync
import com.oblac.nomen.Nomen
import com.raphtory.internals.management.GraphConfig.ConfigBuilder
import com.raphtory.internals.management.id.LocalIDManager
import com.raphtory.internals.management.id.ZooKeeperCounter
import com.raphtory.internals.management.id.ZookeeperLimitedPool
import com.typesafe.config.Config

package object raphtory {

  private[raphtory] def createName: String = Nomen.est().adjective().color().animal().get()

  private[raphtory] val defaultConf: Config  = ConfigBuilder.getDefaultConfig
  private[raphtory] lazy val deployInterface = defaultConf.getString("raphtory.deploy.address")
  private[raphtory] lazy val deployPort      = defaultConf.getInt("raphtory.deploy.port")

  private[raphtory] def makeLocalIdManager[IO[_]: Sync]: Resource[IO, LocalIDManager] =
    Resource.eval(Sync[IO].delay(new LocalIDManager))

  private[raphtory] def makePartitionIDManager[IO[_]: Sync](config: Config): Resource[IO, ZookeeperLimitedPool] = {
    val zookeeperAddress         = config.getString("raphtory.zookeeper.address")
    val partitionServers: Int    = config.getInt("raphtory.partitions.serverCount")
    val partitionsPerServer: Int = config.getInt("raphtory.partitions.countPerServer")
    val totalPartitions: Int     = partitionServers * partitionsPerServer
    ZookeeperLimitedPool(zookeeperAddress, "partitionCount", poolSize = totalPartitions)
  }

  private[raphtory] def makeSourceIDManager[IO[_]: Sync](config: Config): Resource[IO, ZooKeeperCounter] = { //Currently no reason to use as the head node is the authority
    val zookeeperAddress = config.getString("raphtory.zookeeper.address")
    ZooKeeperCounter(zookeeperAddress, "sourceCount")
  }

}
