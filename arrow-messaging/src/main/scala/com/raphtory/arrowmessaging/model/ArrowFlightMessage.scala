package com.raphtory.arrowmessaging.model

import com.raphtory.arrowmessaging.shapelessarrow._
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.complex.impl.UnionListWriter

import java.util.concurrent.ConcurrentHashMap
import scala.reflect.ClassTag

trait ArrowFlightMessage

trait ArrowFlightMessageVectors

abstract class ArrowFlightMessageSchema[A <: ArrowFlightMessageVectors,
  B <: ArrowFlightMessage](
                            val vectorSchemaRoot: VectorSchemaRoot,
                            val vectors: A
                          )(
                            implicit
                            val an: AllocateNew[A],
                            val ss: SetSafe[A, B],
                            val svc: SetValueCount[A],
                            val is: IsSet[A],
                            val g: Get[A, B],
                            val c: Close[A],
                            val ct: ClassTag[B]
                          ) extends AutoCloseable {

  private val listVectorToWriter = new ConcurrentHashMap[ListVector, UnionListWriter]()

  private var row: Int = -1

  final def allocateNew(): Unit = an.allocateNew(vectors)

  final def addMessages(message: ArrowFlightMessage): Unit = {
    def addMessages(f: Int => Unit): Unit = {
      row = row + 1
      f(row)
    }

    val msg = message.asInstanceOf[B]
    addMessages((row: Int) =>
      ss.setSafe(vectors, row, msg)(listVectorToWriter)
    )
  }

  final def completeAddMessages(): Unit = {
    def completeAddMessages(f: Int => Unit): Unit = {
      row = row + 1
      f(row)
      vectorSchemaRoot.setRowCount(row)
      row = -1
    }

    completeAddMessages((row: Int) =>
      svc.setValueCount(vectors, row)
    )
  }

  final def isMessageExistsAtRow(row: Int): Boolean =
    is.isSet(vectors, row).forall(_ == 1)

  final def getMessageAtRow(row: Int): B =
    g.invokeGet(vectors, row)

  def decodeMessage[T](row: Int): T

  def encodeMessage[T](msg: T): ArrowFlightMessage

  final override def close(): Unit = {
    vectorSchemaRoot.close()
    c.close(vectors)
  }

  final def clear(): Unit = listVectorToWriter.clear()

}

trait ArrowFlightMessageSchemaFactory {

  def getInstance(allocator: BufferAllocator): ArrowFlightMessageSchema[_, _]

  def getInstance(vectorSchemaRoot: VectorSchemaRoot): ArrowFlightMessageSchema[_, _]

}
