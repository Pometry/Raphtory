package com.raphtory.arrowmessaging

import org.scalatest.funsuite.AnyFunSuite

class ArrowFlightMessageSignatureRegistryTestSuite extends AnyFunSuite {

  private val registry = ArrowFlightMessageSignatureRegistry()

  test("Register a given a message schema factory class against an endpoint ") {
    assert(!registry.contains("messages"))
    registry.registerSignature("messages", classOf[MixArrowFlightMessage])
    assert(registry.contains("messages"))
  }

  test("Validate correct message schema factory class is registered") {
    assert(registry.getSignature("messages").schemaFactoryClass == classOf[MixArrowFlightMessageSchemaFactory])
  }

  test("Entry against a given enpoint is removed if already present") {
    assert(registry.contains("messages"))
    registry.deregisterSignature("messages")
    assert(!registry.contains("messages"))
  }

  test("Throws exception of a message type for which there is no schema factory defined") {
    assertThrows[ClassNotFoundException](registry.registerSignature("fake", classOf[FakeMessage]))
  }

}
