package com.raphtory.internals.components.registries

import cats.effect.IO
import com.dimafeng.testcontainers.GenericContainer.DockerImage
import com.dimafeng.testcontainers.ContainerDef
import com.dimafeng.testcontainers.GenericContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import munit.CatsEffectSuite
import org.apache.curator.framework.CuratorFramework

class ServiceDiscoveryTest extends CatsEffectSuite with TestContainerForAll {

  override val containerDef: ContainerDef =
    GenericContainer.Def(dockerImage = DockerImage(Left("zookeeper")), exposedPorts = List(2181))

  def withZk[A](f: ServiceDiscovery[IO] => A): A =
    withContainers {
      case c: GenericContainer =>
        val port                        = c.underlyingUnsafeContainer.getMappedPort(2181)
        val host                        = c.underlyingUnsafeContainer.getHost
        val uri                         = s"$host:$port"
        val framework: CuratorFramework = DistributedServiceRegistry.buildZkClient(uri)
        framework.start()
        f(new ServiceDiscovery[IO](framework))
    }

  test("ServiceDiscover will fail on attempting to register the same id twice") {
    withZk { sd =>
      for {
        _      <- sd.unregisterService("partition", 7).attempt
        first  <- sd.registerService(ServiceInstance("123.4.5.6", 1234), "partition", 7)
        second <- sd.registerService(ServiceInstance("123.4.5.6", 1234), "partition", 7).attempt
      } yield {
        assertEquals(first, 7)
        assert(second.isLeft)
      }
    }
  }

  test("ServiceDiscover can create ids for same service") {
    withZk { sd =>
      for {
        _      <- sd.unregisterService("query", 1).attempt
        _      <- sd.unregisterService("query", 2).attempt
        _      <- sd.unregisterService("query", 3).attempt
        first  <- sd.registerService(ServiceInstance("123.4.5.6", 1234), "query", 1)
        second <- sd.registerService(ServiceInstance("123.4.5.6", 1234), "query", 2)
        third  <- sd.registerService(ServiceInstance("123.4.5.6", 1234), "query", 3)
      } yield {
        assertEquals(first, 1)
        assertEquals(second, 2)
        assertEquals(third, 3)
      }
    }
  }

  test("ServiceDiscover can get the service back after creation") {
    withZk { sd =>
      for {
        _        <- sd.unregisterService("blerg", 1).attempt
        _        <- sd.unregisterService("blerg", 2).attempt
        instance1 = ServiceInstance("123.4.5.6", 1234)
        first    <- sd.registerService(instance1, "blerg", 1)
        instance2 = ServiceInstance("123.4.5.7", 1236)
        second   <- sd.registerService(instance2, "blerg", 2)

        blerg1   <- sd.queryForInstance("blerg", 1)
        blerg2   <- sd.queryForInstance("blerg", 2)
        notFound <- sd.queryForInstance("blergx", 2)
      } yield {
        assertEquals(first, 1)
        assertEquals(second, 2)
        assertEquals(blerg1, Some(instance1))
        assertEquals(blerg2, Some(instance2))
        assertEquals(notFound, None)
      }
    }
  }
}
