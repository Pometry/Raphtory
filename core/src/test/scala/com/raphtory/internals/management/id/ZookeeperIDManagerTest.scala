package com.raphtory.internals.management.id

import cats.effect.IO
import com.raphtory.Raphtory
import munit.CatsEffectSuite

import scala.util.Random

class ZookeeperIDManagerTest extends CatsEffectSuite {

  private val deploymentID     = s"raphtory-test-${Random.nextLong().abs}"
  private val config           = Raphtory.getDefaultConfig()
  private val zookeeperAddress = config.getString("raphtory.zookeeper.address")

  private val manager =
    ResourceFixture(ZookeeperIDManager[IO](zookeeperAddress, deploymentID, "testCounter", 4))

  manager.test("Different ZookeeperIDManager instances return different ids and no greater than the limit") { zk =>
    val ids = Set.fill(4)(zk.getNextAvailableID()).collect { case Some(id) => id }
    assertEquals(ids, Set(0, 1, 2, 3))
    assertEquals(zk.getNextAvailableID(), None)
  }

}
