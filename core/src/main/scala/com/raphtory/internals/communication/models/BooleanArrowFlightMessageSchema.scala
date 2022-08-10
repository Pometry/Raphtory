package com.raphtory.internals.communication.models

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo._
import org.apache.arrow.vector.VectorSchemaRoot
import com.raphtory.arrowmessaging.model._
import com.raphtory.arrowmessaging.shapelessarrow._
import com.raphtory.internals.components.querymanager.VertexMessage
import com.raphtory.internals.communication.SchemaProviderInstances._
import scala.reflect.ClassTag

case class BooleanArrowFlightMessage(
    superStep: Int = 0,
    dstVertexId: Long = 0L,
    data: Boolean = false
) extends ArrowFlightMessage

case class BooleanArrowFlightMessageVectors(
    superSteps: IntVector,
    dstVertexIds: BigIntVector,
    data: BitVector
) extends ArrowFlightMessageVectors

case class BooleanArrowFlightMessageSchema[
    A <: ArrowFlightMessageVectors,
    B <: ArrowFlightMessage
] private (
    override val vectorSchemaRoot: VectorSchemaRoot,
    override val vectors: A
)(implicit
    override val an: AllocateNew[A],
    override val ss: SetSafe[A, B],
    override val svc: SetValueCount[A],
    override val is: IsSet[A],
    override val g: Get[A, B],
    override val c: Close[A],
    override val ct: ClassTag[B]
) extends ArrowFlightMessageSchema[A, B](vectorSchemaRoot, vectors) {

  override def decodeMessage[T](row: Int): T = {
    val msg = getMessageAtRow(row).asInstanceOf[BooleanArrowFlightMessage]
    VertexMessage(msg.superStep, msg.dstVertexId, msg.data).asInstanceOf[T]
  }

  override def encodeMessage[T](msg: T): ArrowFlightMessage = {
    val fmsg = msg.asInstanceOf[VertexMessage[_, _]]
    BooleanArrowFlightMessage(
            fmsg.superstep,
            fmsg.vertexId.asInstanceOf[Long],
            fmsg.data.asInstanceOf[Boolean]
    )
  }
}

class BooleanArrowFlightMessageSchemaFactory extends ArrowFlightMessageSchemaFactory {

  private def getVectors(vectorSchemaRoot: VectorSchemaRoot) = {
    val superSteps   = vectorSchemaRoot.getVector("superStep").asInstanceOf[IntVector]
    val dstVertexIds = vectorSchemaRoot.getVector("dstVertexId").asInstanceOf[BigIntVector]
    val data         = vectorSchemaRoot.getVector("data").asInstanceOf[BitVector]

    (superSteps, dstVertexIds, data)
  }

  override def getInstance(
      allocator: BufferAllocator
  ): BooleanArrowFlightMessageSchema[
          BooleanArrowFlightMessageVectors,
          BooleanArrowFlightMessage
  ] = {
    import scala.jdk.CollectionConverters._

    val schema: Schema =
      new Schema(
              List(
                      new Field(
                              "superStep",
                              new FieldType(false, new ArrowType.Int(32, true), null),
                              null
                      ),
                      new Field(
                              "dstVertexId",
                              new FieldType(false, new ArrowType.Int(64, true), null),
                              null
                      ),
                      new Field("data", new FieldType(false, new ArrowType.Bool(), null), null)
              ).asJava
      )

    val vectorSchemaRoot                 = VectorSchemaRoot.create(schema, allocator)
    val (superSteps, dstVertexIds, data) = getVectors(vectorSchemaRoot)

    BooleanArrowFlightMessageSchema(
            vectorSchemaRoot,
            BooleanArrowFlightMessageVectors(
                    superSteps,
                    dstVertexIds,
                    data
            )
    )
  }

  override def getInstance(
      vectorSchemaRoot: VectorSchemaRoot
  ): BooleanArrowFlightMessageSchema[
          BooleanArrowFlightMessageVectors,
          BooleanArrowFlightMessage
  ] = {
    val (superSteps, dstVertexIds, data) = getVectors(vectorSchemaRoot)

    BooleanArrowFlightMessageSchema(
            vectorSchemaRoot,
            BooleanArrowFlightMessageVectors(
                    superSteps,
                    dstVertexIds,
                    data
            )
    )
  }
}
