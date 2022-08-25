package com.raphtory.internals.communication.models.vertexmessaging

import com.raphtory.arrowmessaging.model._
import com.raphtory.arrowmessaging.shapelessarrow._
import com.raphtory.internals.communication.SchemaProviderInstances._
import com.raphtory.internals.components.querymanager.VertexMessage
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo._

import scala.reflect.ClassTag

case class LongArrowFlightMessage(
    superStep: Int = 0,
    dstVertexId: Long = 0L,
    vertexId: Long = 0L
) extends ArrowFlightMessage

case class LongArrowFlightMessageVectors(
    superSteps: IntVector,
    dstVertexIds: BigIntVector,
    vertexIds: BigIntVector
) extends ArrowFlightMessageVectors

case class LongArrowFlightMessageSchema[
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
    val msg = getMessageAtRow(row).asInstanceOf[LongArrowFlightMessage]
    VertexMessage(msg.superStep, msg.dstVertexId, msg.vertexId).asInstanceOf[T]
  }

  override def encodeMessage[T](msg: T): ArrowFlightMessage = {
    val fmsg = msg.asInstanceOf[VertexMessage[_, _]]
    LongArrowFlightMessage(
            fmsg.superstep,
            fmsg.vertexId.asInstanceOf[Long],
            fmsg.data.asInstanceOf[Long]
    )
  }
}

class LongArrowFlightMessageSchemaFactory extends ArrowFlightMessageSchemaFactory {

  private def getVectors(vectorSchemaRoot: VectorSchemaRoot) = {
    val superSteps   = vectorSchemaRoot.getVector("superStep").asInstanceOf[IntVector]
    val dstVertexIds = vectorSchemaRoot.getVector("dstVertexId").asInstanceOf[BigIntVector]
    val vertexIds    = vectorSchemaRoot.getVector("vertexId").asInstanceOf[BigIntVector]

    (superSteps, dstVertexIds, vertexIds)
  }

  override def getInstance(
      allocator: BufferAllocator
  ): LongArrowFlightMessageSchema[LongArrowFlightMessageVectors, LongArrowFlightMessage] = {
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
                      new Field(
                              "vertexId",
                              new FieldType(false, new ArrowType.Int(64, true), null),
                              null
                      )
              ).asJava
      )

    val vectorSchemaRoot                      = VectorSchemaRoot.create(schema, allocator)
    val (superSteps, dstVertexIds, vertexIds) = getVectors(vectorSchemaRoot)

    LongArrowFlightMessageSchema(
            vectorSchemaRoot,
            LongArrowFlightMessageVectors(
                    superSteps,
                    dstVertexIds,
                    vertexIds
            )
    )
  }

  override def getInstance(
      vectorSchemaRoot: VectorSchemaRoot
  ): LongArrowFlightMessageSchema[LongArrowFlightMessageVectors, LongArrowFlightMessage] = {
    val (superSteps, dstVertexIds, vertexIds) = getVectors(vectorSchemaRoot)

    LongArrowFlightMessageSchema(
            vectorSchemaRoot,
            LongArrowFlightMessageVectors(
                    superSteps,
                    dstVertexIds,
                    vertexIds
            )
    )
  }
}
