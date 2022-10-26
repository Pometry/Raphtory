package com.raphtory.internals.components.repositories

import cats.effect.Async
import cats.effect.Resource
import cats.syntax.all._
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

class DistributedServiceRepository[F[_]: Async](topics: TopicRepository, serviceDiscovery: ServiceDiscovery[Void])
        extends ServiceRepository[F](topics) {

  override protected def getService[T](descriptor: ServiceDescriptor[F, T], id: Int): Resource[F, T] = {
    val instance = serviceDiscovery.queryForInstance(descriptor.name, id.toString)
    descriptor.makeClient(instance.getAddress, instance.getPort)
  }

  override protected def register[T](instance: T, descriptor: ServiceDescriptor[F, T], id: Int): F[F[Unit]] =
    for {
      serverResource    <- descriptor.makeServer(instance).allocated
      (port, stopServer) = serverResource
      zkInstance         = serviceInstance(descriptor.name, id, port)
      _                 <- Async[F].blocking(serviceDiscovery.registerService(zkInstance))
      unregister        <- Async[F].pure(for {
                             _ <- Async[F].blocking(serviceDiscovery.unregisterService(zkInstance))
                             _ <- stopServer
                           } yield ())
    } yield unregister

  private def serviceInstance(name: String, id: Int, port: Int): ServiceInstance[Void] =
    ServiceInstance
      .builder()
      .address(InetAddress.getLocalHost.getHostAddress)
      .port(port)
      .name("service")
      .id(s"$name-${id.toString}") // The id needs to be unique among different service names, so we include 'name'
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
