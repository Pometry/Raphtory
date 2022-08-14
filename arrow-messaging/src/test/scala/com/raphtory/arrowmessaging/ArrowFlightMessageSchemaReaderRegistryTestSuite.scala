package com.raphtory.arrowmessaging

import com.raphtory.arrowmessaging
import com.raphtory.arrowmessaging.arrowmessaging.allocator
import com.raphtory.arrowmessaging.arrowmessaging.schema
import org.apache.arrow.vector.VectorSchemaRoot
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class ArrowFlightMessageSchemaReaderRegistryTestSuite extends AnyFunSuite with BeforeAndAfterAll {

  private val registry = ArrowFlightMessageSignatureRegistry()
  registry.registerSignature("messages", classOf[MixArrowFlightMessage])

  private val schemaReaderRegistry = ArrowFlightMessageSchemaReaderRegistryMock(registry)
  private val vectorSchemaRoot     = VectorSchemaRoot.create(schema, allocator)

  override protected def afterAll(): Unit = schemaReaderRegistry.close()

  test(
          "Reader schema registry creates and return instance of schema registered against an endpoint and vectorSchemaRoot instance"
  ) {
    assert(
            schemaReaderRegistry
              .getSchema("messages", vectorSchemaRoot)
              .isInstanceOf[arrowmessaging.MixArrowFlightMessageSchema[_, _]]
    )
  }

  test("Reader schema registery throws an exception for an endpoint against which there is no schema registered") {
    assertThrows[NullPointerException](schemaReaderRegistry.getSchema("unregistered", vectorSchemaRoot))
  }

}
