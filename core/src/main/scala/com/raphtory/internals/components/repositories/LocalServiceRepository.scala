package com.raphtory.internals.components.repositories

import cats.effect.Async
import cats.effect.Ref
import cats.effect.Resource
import cats.syntax.all._
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.ServiceDescriptor
import com.raphtory.internals.components.ServiceRepository

class LocalServiceRepository[F[_]: Async](topics: TopicRepository, services: Ref[F, Map[String, Any]])
        extends ServiceRepository[F](topics) {

  override def registered[T](instance: T, descriptor: ServiceDescriptor[F, T]): Resource[F, Unit] =
    Resource.make(services.update(services => services + (descriptor.name -> instance)))(_ =>
      services.update(services => services - descriptor.name) // TODO: this doesn't work for multiple instances
    )

  override def getService[T](descriptor: ServiceDescriptor[F, T]): Resource[F, T] =
    Resource.eval(services.get.map(serviceList => serviceList(descriptor.name).asInstanceOf[T]))
}

object LocalServiceRepository {

  def apply[F[_]: Async](topics: TopicRepository): Resource[F, ServiceRepository[F]] =
    for {
      services <- Resource.eval(Ref.of(Map[String, Any]()))
      repo     <- Resource.eval(Async[F].delay(new LocalServiceRepository[F](topics, services)))
    } yield repo
}
