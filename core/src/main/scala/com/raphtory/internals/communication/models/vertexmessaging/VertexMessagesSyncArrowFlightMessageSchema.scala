package com.raphtory.internals.communication.models.vertexmessaging

import com.raphtory.arrowmessaging.model._
import com.raphtory.arrowmessaging.shapelessarrow._
import com.raphtory.internals.communication.SchemaProviderInstances._
import com.raphtory.internals.components.querymanager.VertexMessagesSync
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo._

import scala.reflect.ClassTag

case class VertexMessagesSyncArrowFlightMessage(
    partitionID: Int = 0,
    count: Long = 0L
) extends ArrowFlightMessage

case class VertexMessagesSyncArrowFlightMessageVectors(
    partitionIDs: IntVector,
    counts: BigIntVector
) extends ArrowFlightMessageVectors

case class VertexMessagesSyncArrowFlightMessageSchema[
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
    val msg = getMessageAtRow(row).asInstanceOf[VertexMessagesSyncArrowFlightMessage]
    VertexMessagesSync(msg.partitionID, msg.count).asInstanceOf[T]
  }

  override def encodeMessage[T](msg: T): ArrowFlightMessage = {
    val fmsg = msg.asInstanceOf[VertexMessagesSync]
    VertexMessagesSyncArrowFlightMessage(
            fmsg.partitionID,
            fmsg.count
    )
  }
}

class VertexMessagesSyncArrowFlightMessageSchemaFactory extends ArrowFlightMessageSchemaFactory {

  private def getVectors(vectorSchemaRoot: VectorSchemaRoot) = {
    val partitionIDs = vectorSchemaRoot.getVector("partitionID").asInstanceOf[IntVector]
    val counts       = vectorSchemaRoot.getVector("count").asInstanceOf[BigIntVector]

    (partitionIDs, counts)
  }

  override def getInstance(
      allocator: BufferAllocator
  ): VertexMessagesSyncArrowFlightMessageSchema[
          VertexMessagesSyncArrowFlightMessageVectors,
          VertexMessagesSyncArrowFlightMessage
  ] = {
    import scala.jdk.CollectionConverters._

    val schema: Schema =
      new Schema(
              List(
                      new Field(
                              "partitionID",
                              new FieldType(false, new ArrowType.Int(32, true), null),
                              null
                      ),
                      new Field(
                              "count",
                              new FieldType(false, new ArrowType.Int(64, true), null),
                              null
                      )
              ).asJava
      )

    val vectorSchemaRoot       = VectorSchemaRoot.create(schema, allocator)
    val (partitionIDs, counts) = getVectors(vectorSchemaRoot)

    VertexMessagesSyncArrowFlightMessageSchema(
            vectorSchemaRoot,
            VertexMessagesSyncArrowFlightMessageVectors(
                    partitionIDs,
                    counts
            )
    )
  }

  override def getInstance(
      vectorSchemaRoot: VectorSchemaRoot
  ): VertexMessagesSyncArrowFlightMessageSchema[
          VertexMessagesSyncArrowFlightMessageVectors,
          VertexMessagesSyncArrowFlightMessage
  ] = {
    val (partitionIDs, counts) = getVectors(vectorSchemaRoot)

    VertexMessagesSyncArrowFlightMessageSchema(
            vectorSchemaRoot,
            VertexMessagesSyncArrowFlightMessageVectors(
                    partitionIDs,
                    counts
            )
    )
  }
}
