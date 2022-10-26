package com.raphtory.internals.components.repositories

import cats.effect.Async
import cats.effect.Ref
import cats.effect.Resource
import cats.syntax.all._
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.ServiceDescriptor
import com.raphtory.internals.components.ServiceRepository

/** Local implementation of the ServiceRepository
  * @param topics the legacy TopicRepository to be removed soon
  * @param services Map from (service name, instance ID) to instance
  * @param async$F$0 the implicit Async type class for F
  * @tparam F the effect type
  */
class LocalServiceRepository[F[_]: Async](topics: TopicRepository, services: Ref[F, Map[(String, Int), Any]])
        extends ServiceRepository[F](topics) {

  override protected def register[T](instance: T, descriptor: ServiceDescriptor[F, T], id: Int): F[F[Unit]] =
    services
      .update { services =>
        val key = (descriptor.name, id)
        if (services.contains(key)) throw new RuntimeException("Service already registered")
        else services + (key -> instance)
      }
      .as(services.update(services => services - (descriptor.name, id)))

  override def getService[T](descriptor: ServiceDescriptor[F, T], id: Int = 0): Resource[F, T] =
    Resource.eval(services.get.map(serviceList => serviceList((descriptor.name, id)).asInstanceOf[T]))
}

object LocalServiceRepository {

  def apply[F[_]: Async](topics: TopicRepository): Resource[F, ServiceRepository[F]] =
    for {
      services <- Resource.eval(Ref.of(Map[(String, Int), Any]()))
      repo     <- Resource.eval(Async[F].delay(new LocalServiceRepository[F](topics, services)))
    } yield repo
}
