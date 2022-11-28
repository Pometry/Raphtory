//package com.raphtory.internals.communication.repositories
//
//import cats.effect.Async
//import cats.effect.Resource
//import com.raphtory.internals.communication.TopicRepository
//import com.raphtory.internals.communication.connectors.AkkaConnector
//import com.raphtory.internals.management.arrow.ArrowFlightHostAddressProvider
//import com.typesafe.config.Config
//
//private[raphtory] object LocalTopicRepository {
//
//  def apply[IO[_]: Async](
//      config: Config,
//      addressProvider: Option[ArrowFlightHostAddressProvider]
//  ): Resource[IO, TopicRepository] =
//    config.getString("raphtory.communication.control") match {
//      case "auto" | "akka" =>
//        for {
//          akkaConnector <- AkkaConnector[IO](AkkaConnector.StandaloneMode, config)
//        } yield new TopicRepository(akkaConnector, akkaConnector, akkaConnector, config)
//    }
//
//}
