package com.raphtory.internals.components.repositories

import cats.effect.Async
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.KeeperException

import scala.util.control.NonFatal

class ServiceDiscovery[F[_]](zk: CuratorFramework)(implicit F: Async[F]) {

  private val om = new ObjectMapper()
  om.registerModule(DefaultScalaModule)
  // /discovery/name-0
  // /discovery/name-1
  // /discovery/name-2
  // etc..

  def registerService(serviceInstance: ServiceInstance, name: String, id: Int): F[Int] =
    F.blocking {
      try {
        zk.create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.EPHEMERAL)
          .forPath(mkPath(name, id), om.writeValueAsBytes(serviceInstance))
      } catch {
        case NonFatal(t) =>
          t.printStackTrace()
          throw t
      }
      id
    }

  private def mkPath(name: String, id: Int) =
    s"/discovery/$name-$id"

  def queryForInstance(name: String, id: Int): F[Option[ServiceInstance]] =
    F.blocking {
      try {
        val bytes = zk.getData.forPath(mkPath(name, id))
        Some(om.readValue(bytes, classOf[ServiceInstance]))
      }
      catch {
        case NonFatal(t: KeeperException.NoNodeException) =>
          None
      }
    }

  def unregisterService(name: String, id: Int): F[Unit] =
    F.blocking {
      zk.delete().forPath(mkPath(name, id))
    }

}

case class ServiceInstance(address: String, port: Int)
