package com.raphtory.internals.management.id

import com.raphtory.Raphtory
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

class ZookeeperIDManagerTest extends AnyFunSuite {
  test("Different ZookeeperIDManager instances return different ids and no greater than the limit") {
    val deploymentID = s"raphtory-test-${Random.nextLong().abs}"
    val ids          = Set.fill(4)(getNextAvailableID(deploymentID)).collect { case Some(id) => id }
    assert(ids === Set(0, 1, 2, 3))
    assert(getNextAvailableID(deploymentID) === None)
  }

  private def getNextAvailableID(deploymentID: String): Option[Int] = {
    val config           = Raphtory.getDefaultConfig()
    val zookeeperAddress = config.getString("raphtory.zookeeper.address")
    val manager          =
      ZookeeperIDManager(zookeeperAddress, deploymentID, "testCounter", 4)
    manager.getNextAvailableID()
  }
}
