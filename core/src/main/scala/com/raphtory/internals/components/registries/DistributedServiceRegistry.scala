package com.raphtory.internals.components.registries

import cats.effect.Async
import cats.effect.Resource
import cats.syntax.all._
import com.raphtory.internals.components.ServiceDescriptor
import com.raphtory.internals.components.ServiceRegistry
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.slf4j.LoggerFactory

import java.net.InetAddress
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

class DistributedServiceRegistry[F[_]: Async](serviceDiscovery: ServiceDiscovery[F]) extends ServiceRegistry[F] {

  private val log: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private def serviceId(name: String, id: Int) = s"$name-${id.toString}"
  private def serviceName                      = "service"

  override protected def getService[T](descriptor: ServiceDescriptor[F, T], id: Int): Resource[F, T] = {
    def getServiceRetry[R](
        descriptor: ServiceDescriptor[F, R],
        id: Int,
        waitBeforeRetry: FiniteDuration,
        count: Int,
        attempt: Int = 1
    ): F[ServiceInstance] = {
      val name = serviceId(descriptor.name, id)
      serviceDiscovery
        .queryForInstance(descriptor.name, id)
        .flatMap {
          case None if count > 0 =>
            log.warn(s"Searching for service-name: $serviceName, name: $name attempt $attempt")
            Async[F]
              .sleep(waitBeforeRetry) *> getServiceRetry(descriptor, id, waitBeforeRetry * 2, count - 1, attempt + 1)
          case Some(v)           =>
            Async[F].pure(v)
          case _                 =>
            Async[F].raiseError(
                    new IllegalStateException(s"Failed to find service-name: $serviceName, name: $name giving up")
            )
        }
    }

    for {
      instance <- Resource.eval(getServiceRetry(descriptor, id, 1.seconds, 15))
      client   <- descriptor.makeClient(instance.address, instance.port)
    } yield client
  }

  override protected def register[T](instance: T, descriptor: ServiceDescriptor[F, T], id: Int): F[F[Unit]] = {
    for {
      serverResource    <- descriptor.makeServer(instance).allocated
      (port, stopServer) = serverResource
      zkInstance         = ServiceInstance(InetAddress.getLocalHost.getHostAddress, port)
      _                 <- serviceDiscovery.registerService(zkInstance, descriptor.name, id)
      unregister        <- Async[F].pure(for {
                             _ <- serviceDiscovery.unregisterService(descriptor.name, id)
                             _ <- stopServer
                           } yield ())
    } yield unregister
  }
}

object DistributedServiceRegistry {

  def apply[F[_]: Async](config: Config): Resource[F, ServiceRegistry[F]] =
    for {
      address     <- Resource.pure(config.getString("raphtory.zookeeper.address"))
      client      <- Resource.fromAutoCloseable(Async[F].delay(buildZkClient(address)))
      _           <- Resource.eval(Async[F].blocking(client.start()))
      serviceRepo <- Resource.eval(Async[F].delay(new DistributedServiceRegistry[F](new ServiceDiscovery[F](client))))
    } yield serviceRepo

  def buildZkClient(address: String): CuratorFramework =
    CuratorFrameworkFactory
      .builder()
      .connectString(address)
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .build()
}
