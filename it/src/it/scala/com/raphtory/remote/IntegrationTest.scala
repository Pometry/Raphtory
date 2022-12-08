package com.raphtory.remote

import cats.effect.IO
import com.dimafeng.testcontainers.{DockerComposeContainer, ExposedService}
import com.dimafeng.testcontainers.munit.TestContainerForAll
import munit.IgnoreSuite

import java.io.File

//@IgnoreSuite
class IntegrationTest extends munit.CatsEffectSuite with TestContainerForAll {

  override val containerDef: DockerComposeContainer.Def = DockerComposeContainer
    .Def(composeFiles = new File("docker-compose.yml"), exposedServices = Seq(ExposedService("cluster", 1736)))

  test("I can start raphtory cluster and get the cluster port") {
    withContainers { c: DockerComposeContainer =>
      IO {
        assert(c.getServicePort("cluster", 1736) > 0)
      }
    }
  }
}
