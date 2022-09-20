package com.raphtory

import cats.effect.IO
import com.raphtory.internals.management.id.ZookeeperLimitedPool
import munit.CatsEffectSuite
import munit.IgnoreSuite

import scala.util.Random

class ZookeeperIDManagerTest extends CatsEffectSuite {

  private val deploymentID     = s"raphtory-test-${Random.nextLong().abs}"
  private val config           = Raphtory.getDefaultConfig()
  private val zookeeperAddress = config.getString("raphtory.zookeeper.address")

  private val manager =
    ResourceFixture(ZookeeperLimitedPool[IO](zookeeperAddress, deploymentID, "testCounter", 4))

  manager.test("Different ZookeeperIDManager instances return different ids and no greater than the limit") { zk =>
    val ids = Set.fill(4)(zk.getNextAvailableID()).collect { case Some(id) => id }
    assertEquals(ids, Set(0, 1, 2, 3))
    assertEquals(zk.getNextAvailableID(), None)
  }

}
