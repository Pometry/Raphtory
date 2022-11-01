package com.raphtory.config

import com.typesafe.config.ConfigFactory
import munit.CatsEffectSuite

import java.io.File

class DefaultConfigTest extends CatsEffectSuite {
  test("Default application.conf arguments remain unchanged") {
    val config = ConfigFactory.parseFile(new File("core/src/main/resources/application.conf")).resolve()
    assertEquals(config.getInt("raphtory.partitions.countPerServer"), 1)
    assertEquals(config.getInt("raphtory.partitions.serverCount"), 1)
  }

  test("Default application.conf arguments for testing remain unchanged") {
    val config = ConfigFactory.parseFile(new File("core/src/test/resources/application.conf")).resolve()
    assertEquals(config.getInt("raphtory.partitions.countPerServer"), 3)
    assertEquals(config.getInt("raphtory.partitions.serverCount"), 1)
  }
}
