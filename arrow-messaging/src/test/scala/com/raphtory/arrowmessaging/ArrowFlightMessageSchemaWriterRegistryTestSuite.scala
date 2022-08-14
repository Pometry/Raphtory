package com.raphtory.arrowmessaging

import com.raphtory.arrowmessaging
import com.raphtory.arrowmessaging.arrowmessaging.allocator
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class ArrowFlightMessageSchemaWriterRegistryTestSuite extends AnyFunSuite with BeforeAndAfterAll {

  private val registry = ArrowFlightMessageSignatureRegistry()
  registry.registerSignature("messages", classOf[MixArrowFlightMessage])

  private val schemaWriterRegistry = ArrowFlightMessageSchemaWriterRegistryMock(allocator, registry)

  override protected def afterAll(): Unit = schemaWriterRegistry.close()

  test("Writer schema registry creates and return instance of schema registered against an endpoint") {
    assert(schemaWriterRegistry.getSchema("messages").isInstanceOf[arrowmessaging.MixArrowFlightMessageSchema[_, _]])
  }

  test("Writer schema registery throws an exception for an endpoint against which there is no schema registered") {
    assertThrows[NullPointerException](schemaWriterRegistry.getSchema("unregistered"))
  }

}
