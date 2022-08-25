package com.raphtory.internals.communication.models.vertexmessaging

import com.raphtory.arrowmessaging.model._
import com.raphtory.arrowmessaging.shapelessarrow._
import com.raphtory.internals.communication.SchemaProviderInstances._
import com.raphtory.internals.components.querymanager._
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo._

import scala.reflect.ClassTag

case class FilteredEdgeMessageArrowFlightMessage(
    superStep: Int = 0,
    vertexId: Long = 0L,
    srcId: Long = 0L
) extends ArrowFlightMessage

case class FilteredEdgeMessageArrowFlightMessageVectors(
    superSteps: IntVector,
    vertexIds: BigIntVector,
    srcIds: BigIntVector
) extends ArrowFlightMessageVectors

case class FilteredEdgeMessageArrowFlightMessageSchema[
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
    val msg = getMessageAtRow(row).asInstanceOf[FilteredEdgeMessageArrowFlightMessage]
    FilteredEdgeMessage(msg.superStep, msg.vertexId, msg.srcId).asInstanceOf[T]
  }

  override def encodeMessage[T](msg: T): ArrowFlightMessage = {
    val fmsg = msg.asInstanceOf[FilteredEdgeMessage[_]]
    FilteredEdgeMessageArrowFlightMessage(
            fmsg.superstep,
            fmsg.vertexId.asInstanceOf[Long],
            fmsg.sourceId.asInstanceOf[Long]
    )
  }
}

class FilteredEdgeMessageArrowFlightMessageSchemaFactory extends ArrowFlightMessageSchemaFactory {

  private def getVectors(vectorSchemaRoot: VectorSchemaRoot) = {
    val superSteps = vectorSchemaRoot.getVector("superStep").asInstanceOf[IntVector]
    val vertexIds  = vectorSchemaRoot.getVector("vertexId").asInstanceOf[BigIntVector]
    val srcIds     = vectorSchemaRoot.getVector("srcId").asInstanceOf[BigIntVector]

    (superSteps, vertexIds, srcIds)
  }

  override def getInstance(
      allocator: BufferAllocator
  ): FilteredEdgeMessageArrowFlightMessageSchema[
          FilteredEdgeMessageArrowFlightMessageVectors,
          FilteredEdgeMessageArrowFlightMessage
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
                              "vertexId",
                              new FieldType(false, new ArrowType.Int(64, true), null),
                              null
                      ),
                      new Field(
                              "srcId",
                              new FieldType(false, new ArrowType.Int(64, true), null),
                              null
                      )
              ).asJava
      )

    val vectorSchemaRoot                = VectorSchemaRoot.create(schema, allocator)
    val (superSteps, vertexIds, srcIds) = getVectors(vectorSchemaRoot)

    FilteredEdgeMessageArrowFlightMessageSchema(
            vectorSchemaRoot,
            FilteredEdgeMessageArrowFlightMessageVectors(
                    superSteps,
                    vertexIds,
                    srcIds
            )
    )
  }

  override def getInstance(
      vectorSchemaRoot: VectorSchemaRoot
  ): FilteredEdgeMessageArrowFlightMessageSchema[
          FilteredEdgeMessageArrowFlightMessageVectors,
          FilteredEdgeMessageArrowFlightMessage
  ] = {
    val (superSteps, vertexIds, srcIds) = getVectors(vectorSchemaRoot)

    FilteredEdgeMessageArrowFlightMessageSchema(
            vectorSchemaRoot,
            FilteredEdgeMessageArrowFlightMessageVectors(
                    superSteps,
                    vertexIds,
                    srcIds
            )
    )
  }
}
