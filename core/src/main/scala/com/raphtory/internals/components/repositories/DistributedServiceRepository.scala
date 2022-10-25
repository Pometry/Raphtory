package com.raphtory.internals.components.repositories

import cats.effect.Async
import cats.effect.Resource
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.ServiceDescriptor
import com.raphtory.internals.components.ServiceRepository
import com.typesafe.config.Config
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.x.discovery.ServiceDiscovery
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder
import org.apache.curator.x.discovery.ServiceInstance

import java.net.InetAddress
import scala.jdk.CollectionConverters._

class DistributedServiceRepository[F[_]: Async](topics: TopicRepository, serviceDiscovery: ServiceDiscovery[Void])
        extends ServiceRepository[F](topics) {

  def registered[T](instance: T, descriptor: ServiceDescriptor[F, T]): Resource[F, Unit] =
    for {
      port <- descriptor.makeServer(instance)
      _    <- Resource.make(register(descriptor, port))(_ => unregister(descriptor, port))
    } yield ()

  override protected def getService[T](descriptor: ServiceDescriptor[F, T]): Resource[F, T] = {
    val instance = serviceDiscovery.queryForInstances(descriptor.name).asScala.head // TODO: this can fail
    descriptor.makeClient(instance.getAddress, instance.getPort)
  }

  private def register[T](descriptor: ServiceDescriptor[F, T], port: Int): F[Unit] =
    Async[F].blocking {
      val instance = serviceInstance(descriptor.name, port)
      serviceDiscovery.registerService(instance)
    }

  private def unregister[T](descriptor: ServiceDescriptor[F, T], port: Int): F[Unit] =
    Async[F].blocking {
      val instance = serviceInstance(descriptor.name, port)
      serviceDiscovery.unregisterService(instance)
    }

  private def serviceInstance(name: String, port: Int): ServiceInstance[Void] =
    ServiceInstance
      .builder()
      .address(InetAddress.getLocalHost.getHostAddress)
      .port(port)
      .name(name)
      .build()
}

object DistributedServiceRepository {

  def apply[F[_]: Async](topics: TopicRepository, config: Config): Resource[F, ServiceRepository[F]] =
    for {
      address          <- Resource.pure(config.getString("raphtory.zookeeper.address"))
      client           <- Resource.fromAutoCloseable(Async[F].delay(buildZkClient(address)))
      _                <- Resource.eval(Async[F].blocking(client.start()))
      serviceDiscovery <- Resource.fromAutoCloseable(Async[F].delay(buildServiceDiscovery(client)))
      _                <- Resource.eval(Async[F].blocking(serviceDiscovery.start()))
      serviceRepo      <- Resource.eval(Async[F].delay(new DistributedServiceRepository[F](topics, serviceDiscovery)))
    } yield serviceRepo

  private def buildServiceDiscovery(zkClient: CuratorFramework) =
    ServiceDiscoveryBuilder
      .builder(classOf[Void])
      .client(zkClient)
      .basePath("raphtory")
      .build()

  private def buildZkClient(address: String) =
    CuratorFrameworkFactory
      .builder()
      .connectString(address)
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .build()
}
