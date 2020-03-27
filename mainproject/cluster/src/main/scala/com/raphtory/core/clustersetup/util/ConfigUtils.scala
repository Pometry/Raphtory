package com.raphtory.core.clustersetup.util

import com.typesafe.config.Config
import com.typesafe.config.ConfigValue

import scala.collection.JavaConversions._

object ConfigUtils {

  case class SocketAddress(host: String, port: String)

  case class SystemConfig(
      bindAddress: SocketAddress,
      tcpAddress: SocketAddress,
      seeds: List[ConfigValue],
      roles: List[ConfigValue]
  )

  implicit class ConfigParser(config: Config) {
    def parse(): SystemConfig = {
      val bindHost    = config.getString("akka.remote.netty.tcp.bind-hostname")
      val bindPort    = config.getString("akka.remote.netty.tcp.bind-port")
      val bindAddress = SocketAddress(bindHost, bindPort)

      val tcpHost    = config.getString("akka.remote.netty.tcp.hostname")
      val tcpPort    = config.getString("akka.remote.netty.tcp.port")
      val tcpAddress = SocketAddress(tcpHost, tcpPort)

      val seeds = config.getList("akka.cluster.seed-nodes").toList
      val roles = config.getList("akka.cluster.roles").toList

      SystemConfig(bindAddress = bindAddress, tcpAddress = tcpAddress, seeds, roles)
    }
  }
}
