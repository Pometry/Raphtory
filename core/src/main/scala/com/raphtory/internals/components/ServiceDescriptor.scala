package com.raphtory.internals.components

import cats.effect.Async
import cats.effect.Resource
import higherkindness.mu.rpc.ChannelFor
import higherkindness.mu.rpc.ChannelForAddress
import higherkindness.mu.rpc.server.AddService
import higherkindness.mu.rpc.server.GrpcServer
import io.grpc.ServerServiceDefinition

trait ServiceDescriptor[F[_], T] {
  def name: String
  def makeClient(address: String, port: Int): Resource[F, T]

  /** Create a server resource for the service instance
    * @param service the service to put behind the server
    * @return the number of the port allocated
    */
  def makeServer(service: T): Resource[F, Int]
}

abstract class GrpcServiceDescriptor[F[_]: Async, T] extends ServiceDescriptor[F, T] {
  protected def buildGrpcClient(channelFor: ChannelFor): Resource[F, T]
  protected def buildGrpcServer(service: T): Resource[F, ServerServiceDefinition]

  final override def makeClient(address: String, port: Int): Resource[F, T] =
    buildGrpcClient(ChannelForAddress(address, port))

  final override def makeServer(service: T): Resource[F, Int] =
    for {
      serviceDef <- buildGrpcServer(service)
      server     <- Resource.eval(GrpcServer.default[F](0, List(AddService(serviceDef))))
      _          <- GrpcServer.serverResource[F](server)
      port       <- Resource.eval(server.getPort)
    } yield port
}

object GrpcServiceDescriptor {

  def apply[F[_]: Async, T](
      serviceName: String,
      grpcClient: ChannelFor => Resource[F, T],
      grpcServer: T => Resource[F, ServerServiceDefinition]
  ): GrpcServiceDescriptor[F, T] =
    new GrpcServiceDescriptor[F, T] {
      override def name: String                                                                = serviceName
      override protected def buildGrpcClient(channelFor: ChannelFor): Resource[F, T]           = grpcClient(channelFor)
      override protected def buildGrpcServer(service: T): Resource[F, ServerServiceDefinition] = grpcServer(service)
    }
}
