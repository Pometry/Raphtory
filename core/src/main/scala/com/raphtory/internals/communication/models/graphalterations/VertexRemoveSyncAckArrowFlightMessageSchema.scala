package com.raphtory.internals.communication.models.graphalterations

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo._
import org.apache.arrow.vector.VectorSchemaRoot
import com.raphtory.arrowmessaging.model._
import com.raphtory.arrowmessaging.shapelessarrow._
import com.raphtory.internals.graph.GraphAlteration._
import com.raphtory.internals.communication.SchemaProviderInstances._
import scala.reflect.ClassTag

case class VertexRemoveSyncAckArrowFlightMessage(
    sourceID: Int = 0,
    updateTime: Long = 0L,
    index: Long = 0L,
    updateId: Long = 0L
) extends ArrowFlightMessage

case class VertexRemoveSyncAckArrowFlightMessageVectors(
    sourceIDs: IntVector,
    updateTimes: BigIntVector,
    indexes: BigIntVector,
    updateIds: BigIntVector
) extends ArrowFlightMessageVectors

case class VertexRemoveSyncAckArrowFlightMessageSchema[
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
    val msg = getMessageAtRow(row).asInstanceOf[VertexRemoveSyncAckArrowFlightMessage]
    VertexRemoveSyncAck(msg.sourceID, msg.updateTime, msg.index, msg.updateId).asInstanceOf[T]
  }

  override def encodeMessage[T](msg: T): ArrowFlightMessage = {
    val fmsg = msg.asInstanceOf[VertexRemoveSyncAck]
    VertexRemoveSyncAckArrowFlightMessage(
            fmsg.sourceID,
            fmsg.updateTime,
            fmsg.index,
            fmsg.updateId
    )
  }
}

class VertexRemoveSyncAckArrowFlightMessageSchemaFactory extends ArrowFlightMessageSchemaFactory {

  private def getVectors(
      vectorSchemaRoot: VectorSchemaRoot
  ): VertexRemoveSyncAckArrowFlightMessageSchema[
          VertexRemoveSyncAckArrowFlightMessageVectors,
          VertexRemoveSyncAckArrowFlightMessage
  ] = {
    val sourceIDs   = vectorSchemaRoot.getVector("sourceIDs").asInstanceOf[IntVector]
    val updateTimes = vectorSchemaRoot.getVector("updateTimes").asInstanceOf[BigIntVector]
    val indexes     = vectorSchemaRoot.getVector("indexes").asInstanceOf[BigIntVector]
    val updateIds   = vectorSchemaRoot.getVector("updateIds").asInstanceOf[BigIntVector]

    VertexRemoveSyncAckArrowFlightMessageSchema(
            vectorSchemaRoot,
            VertexRemoveSyncAckArrowFlightMessageVectors(
                    sourceIDs,
                    updateTimes,
                    indexes,
                    updateIds
            )
    )
  }

  override def getInstance(
      allocator: BufferAllocator
  ): VertexRemoveSyncAckArrowFlightMessageSchema[
          VertexRemoveSyncAckArrowFlightMessageVectors,
          VertexRemoveSyncAckArrowFlightMessage
  ] = {
    import scala.jdk.CollectionConverters._

    val schema: Schema =
      new Schema(
              List(
                      new Field(
                              "sourceIDs",
                              new FieldType(false, new ArrowType.Int(32, true), null),
                              null
                      ),
                      new Field(
                              "updateTimes",
                              new FieldType(false, new ArrowType.Int(64, true), null),
                              null
                      ),
                      new Field(
                              "indexes",
                              new FieldType(false, new ArrowType.Int(64, true), null),
                              null
                      ),
                      new Field(
                              "updateIds",
                              new FieldType(false, new ArrowType.Int(64, true), null),
                              null
                      )
              ).asJava
      )

    val vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)
    getVectors(vectorSchemaRoot)
  }

  override def getInstance(
      vectorSchemaRoot: VectorSchemaRoot
  ): VertexRemoveSyncAckArrowFlightMessageSchema[
          VertexRemoveSyncAckArrowFlightMessageVectors,
          VertexRemoveSyncAckArrowFlightMessage
  ] =
    getVectors(vectorSchemaRoot)
}
