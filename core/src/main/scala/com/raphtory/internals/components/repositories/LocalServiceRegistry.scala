package com.raphtory.internals.components.repositories

import cats.effect.Async
import cats.effect.Ref
import cats.effect.Resource
import cats.syntax.all._
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.ServiceDescriptor
import com.raphtory.internals.components.ServiceRegistry

import scala.concurrent.duration.DurationInt

/** Local implementation of the ServiceRepository
  * @param topics the legacy TopicRepository to be removed soon
  * @param services Map from (service name, instance ID) to instance
  * @param async$F$0 the implicit Async type class for F
  * @tparam F the effect type
  */
class LocalServiceRegistry[F[_]: Async](topics: TopicRepository, services: Ref[F, Map[(String, Int), Any]])
        extends ServiceRegistry[F](topics) {

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
    Resource.eval(services.get.map(serviceList => serviceList((descriptor.name, id)).asInstanceOf[T]))
}

object LocalServiceRegistry {

  def apply[F[_]: Async](topics: TopicRepository): Resource[F, ServiceRegistry[F]] =
    for {
      services <- Resource.eval(Ref.of(Map[(String, Int), Any]()))
      repo     <- Resource.eval(Async[F].delay(new LocalServiceRegistry[F](topics, services)))
    } yield repo
}
