package com.raphtory.internals.components.repositories

import cats.effect.Async
import cats.effect.Resource
import cats.syntax.all._
import com.raphtory.internals.components.ServiceDescriptor
import com.raphtory.internals.components.ServiceRegistry
import com.typesafe.config.Config
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.x.discovery.ServiceDiscovery
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder
import org.apache.curator.x.discovery.ServiceInstance
import java.net.InetAddress
import scala.concurrent.duration.DurationInt

class DistributedServiceRegistry[F[_]: Async](serviceDiscovery: ServiceDiscovery[Void]) extends ServiceRegistry[F] {

  private def serviceId(name: String, id: Int) = s"$name-${id.toString}"
  private def serviceName                      = "service"

  override protected def getService[T](descriptor: ServiceDescriptor[F, T], id: Int): Resource[F, T] = {
    def getServiceRetry[R](descriptor: ServiceDescriptor[F, R], id: Int): F[ServiceInstance[R]] =
      Async[F]
        .blocking(
                serviceDiscovery
                  .queryForInstance(serviceName, serviceId(descriptor.name, id))
                  .asInstanceOf[ServiceInstance[R]]
        )
        .handleErrorWith(_ => Async[F].delayBy(getServiceRetry(descriptor, id), 10.millis))

    for {
      instance <- Resource.eval(Async[F].timeout(getServiceRetry(descriptor, id), 30.seconds))
      client   <- descriptor.makeClient(instance.getAddress, instance.getPort)
    } yield client
  }

  override protected def register[T](instance: T, descriptor: ServiceDescriptor[F, T], id: Int): F[F[Unit]] = {
    def serviceInstance(name: String, id: Int, port: Int): ServiceInstance[Void] =
      ServiceInstance
        .builder()
        .address(InetAddress.getLocalHost.getHostAddress)
        .port(port)
        .name(serviceName)
        .id(serviceId(name, id)) // The id needs to be unique among different service names, so we include 'name'
        .build()

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
  }
}

object DistributedServiceRegistry {

  def apply[F[_]: Async](config: Config): Resource[F, ServiceRegistry[F]] =
    for {
      address          <- Resource.pure(config.getString("raphtory.zookeeper.address"))
      client           <- Resource.fromAutoCloseable(Async[F].delay(buildZkClient(address)))
      _                <- Resource.eval(Async[F].blocking(client.start()))
      serviceDiscovery <- Resource.fromAutoCloseable(Async[F].delay(buildServiceDiscovery(client)))
      _                <- Resource.eval(Async[F].blocking(serviceDiscovery.start()))
      serviceRepo      <- Resource.eval(Async[F].delay(new DistributedServiceRegistry[F](serviceDiscovery)))
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
