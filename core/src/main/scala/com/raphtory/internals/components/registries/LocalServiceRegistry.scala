package com.raphtory.internals.components.registries

import cats.effect.Async
import cats.effect.Ref
import cats.effect.Resource
import cats.syntax.all._
import com.raphtory.internals.components.ServiceDescriptor
import com.raphtory.internals.components.ServiceRegistry
import com.typesafe.config.Config

import scala.concurrent.duration.DurationInt

/** Local implementation of the ServiceRepository
  * @param topics the legacy TopicRepository to be removed soon
  * @param services Map from (service name, instance ID) to instance
  * @param async$F$0 the implicit Async type class for F
  * @tparam F the effect type
  */
class LocalServiceRegistry[F[_]](services: Ref[F, Map[(String, Int), Any]], conf: Config)(implicit F: Async[F])
        extends ServiceRegistry[F](conf) {

  override protected def register[T](instance: T, descriptor: ServiceDescriptor[F, T], id: Int): F[F[Unit]] = {
    val key = (descriptor.name, id)
    for {
      _ <- services
             .update { services =>
               if (services.contains(key)) throw new IllegalStateException("Service already registered")
               else services + (key -> instance)
             }
      _ <- services.get.map(services =>
             logger.debug(s"Successfully registered '$instance' for id '$id' among services '$services'")
           )
    } yield services.update(services => services - key)
  }

  override def getService[T](descriptor: ServiceDescriptor[F, T], id: Int = 0): Resource[F, T] =
    Resource.eval(
            F.timeout(getServiceOrRetry(descriptor, id), 1.seconds)
              .handleErrorWith { e =>
                F.delay(logger.error(s"Couldn't get a reference to service ${descriptor.name} after 1 second"))
                throw e
              }
    )

  private def getServiceOrRetry[T](descriptor: ServiceDescriptor[F, T], id: Int): F[T] =
    services.get
      .map(serviceList => serviceList((descriptor.name, id)).asInstanceOf[T])
      .handleErrorWith(_ => F.delayBy(getServiceOrRetry(descriptor, id), 10.millis))
}

object LocalServiceRegistry {

  def apply[F[_]: Async](config: Config): Resource[F, ServiceRegistry[F]] =
    for {
      services <- Resource.eval(Ref.of(Map[(String, Int), Any]()))
      repo     <- Resource.eval(Async[F].delay(new LocalServiceRegistry[F](services, config)))
    } yield repo
}
