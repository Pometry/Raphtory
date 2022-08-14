package com.raphtory.arrowmessaging

import com.raphtory.arrowmessaging.arrowmessaging.allocator
import com.raphtory.arrowmessaging.arrowmessaging.mixMessage
import com.raphtory.arrowmessaging.model.ArrowFlightMessage
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class ArrowFlightCommTestSuite extends AnyFunSuite with BeforeAndAfterAll {

  override protected def afterAll(): Unit = {
    writer.close()
    reader.close()
    server.close()
  }

  private val server = ArrowFlightServer(allocator)
  server.waitForServerToStart()

  private val interface = server.getInterface
  private val port      = server.getPort

  implicit val endPoint: String = "messages"

  private val signatureRegistry = ArrowFlightMessageSignatureRegistry()
  signatureRegistry.registerSignature(endPoint, classOf[MixArrowFlightMessage])
  signatureRegistry.registerSignature("messages2", classOf[MixArrowFlightMessage])

  private val reader =
    ArrowFlightReader(
            interface,
            port,
            allocator,
            (msg: ArrowFlightMessage) => assert(msg == mixMessage),
            signatureRegistry
    )

  private val writer = ArrowFlightWriter(interface, port, 0, allocator, signatureRegistry)

  test("Reader reads the message sent by the writer") {
    writer.addToBatch(mixMessage)
    writer.sendBatch
    writer.completeSend
    reader.readMessages()
    assert(reader.getTotalMessagesRead == 1)
  }

  test("Reader records the total number of messages received") {
    writer.addToBatch(mixMessage)
    writer.sendBatch
    writer.completeSend
    Future(reader.readMessages(100))
    Thread.sleep(1000)
    assert(reader.getTotalMessagesRead == 2)
  }

  test("Reader reads all the messages sent by the same writer over multiple endpoints") {
    writer.addToBatch(mixMessage)
    writer.addToBatch(mixMessage)("messages2")
    writer.sendBatch
    writer.completeSend
    reader.readMessages()
    assert(reader.getTotalMessagesRead == 4)
  }

  test("Writer throws exception when attempted to send message over unregistered endpoint") {
    assertThrows[Exception](writer.addToBatch(mixMessage)("unregistered"))
  }

}
