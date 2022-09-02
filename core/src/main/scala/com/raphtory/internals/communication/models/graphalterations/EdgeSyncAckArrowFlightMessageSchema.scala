package com.raphtory.internals.communication.models.graphalterations

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo._
import org.apache.arrow.vector.VectorSchemaRoot
import com.raphtory.arrowmessaging.model._
import com.raphtory.arrowmessaging.shapelessarrow._
import com.raphtory.internals.communication.SchemaProviderInstances._
import com.raphtory.internals.graph.GraphAlteration._

import scala.reflect.ClassTag

case class EdgeSyncAckArrowFlightMessage(
    sourceID: Long = 0L,
    updateTime: Long = 0L,
    index: Long = 0L,
    srcId: Long = 0L,
    dstId: Long = 0L,
    fromAddition: Boolean = false
) extends ArrowFlightMessage

case class EdgeSyncAckArrowFlightMessageVectors(
    sourceIDs: BigIntVector,
    updateTimes: BigIntVector,
    indexes: BigIntVector,
    srcIds: BigIntVector,
    dstIds: BigIntVector,
    fromAdditions: BitVector
) extends ArrowFlightMessageVectors

case class EdgeSyncAckArrowFlightMessageSchema[
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
    val msg = getMessageAtRow(row).asInstanceOf[EdgeSyncAckArrowFlightMessage]
    EdgeSyncAck(msg.sourceID, msg.updateTime, msg.index, msg.srcId, msg.dstId, msg.fromAddition).asInstanceOf[T]
  }

  override def encodeMessage[T](msg: T): ArrowFlightMessage = {
    val fmsg = msg.asInstanceOf[EdgeSyncAck]
    EdgeSyncAckArrowFlightMessage(
            fmsg.sourceID,
            fmsg.updateTime,
            fmsg.index,
            fmsg.srcId,
            fmsg.dstId,
            fmsg.fromAddition
    )
  }
}

class EdgeSyncAckArrowFlightMessageSchemaFactory extends ArrowFlightMessageSchemaFactory {

  private def getVectors(vectorSchemaRoot: VectorSchemaRoot): EdgeSyncAckArrowFlightMessageSchema[
          EdgeSyncAckArrowFlightMessageVectors,
          EdgeSyncAckArrowFlightMessage
  ] = {
    val sourceIDs     = vectorSchemaRoot.getVector("sourceIDs").asInstanceOf[BigIntVector]
    val updateTimes   = vectorSchemaRoot.getVector("updateTimes").asInstanceOf[BigIntVector]
    val indexes       = vectorSchemaRoot.getVector("indexes").asInstanceOf[BigIntVector]
    val srcIds        = vectorSchemaRoot.getVector("srcIds").asInstanceOf[BigIntVector]
    val dstIds        = vectorSchemaRoot.getVector("dstIds").asInstanceOf[BigIntVector]
    val fromAdditions = vectorSchemaRoot.getVector("fromAdditions").asInstanceOf[BitVector]

    EdgeSyncAckArrowFlightMessageSchema(
            vectorSchemaRoot,
            EdgeSyncAckArrowFlightMessageVectors(
                    sourceIDs,
                    updateTimes,
                    indexes,
                    srcIds,
                    dstIds,
                    fromAdditions
            )
    )
  }

  override def getInstance(
      allocator: BufferAllocator
  ): EdgeSyncAckArrowFlightMessageSchema[
          EdgeSyncAckArrowFlightMessageVectors,
          EdgeSyncAckArrowFlightMessage
  ] = {
    import scala.jdk.CollectionConverters._

    val schema: Schema =
      new Schema(
              List(
                      new Field(
                              "sourceIDs",
                              new FieldType(false, new ArrowType.Int(64, true), null),
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
                              "srcIds",
                              new FieldType(false, new ArrowType.Int(64, true), null),
                              null
                      ),
                      new Field(
                              "dstIds",
                              new FieldType(false, new ArrowType.Int(64, true), null),
                              null
                      ),
                      new Field(
                              "fromAdditions",
                              new FieldType(false, new ArrowType.Bool(), null),
                              null
                      )
              ).asJava
      )

    val vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)
    getVectors(vectorSchemaRoot)
  }

  override def getInstance(
      vectorSchemaRoot: VectorSchemaRoot
  ): EdgeSyncAckArrowFlightMessageSchema[
          EdgeSyncAckArrowFlightMessageVectors,
          EdgeSyncAckArrowFlightMessage
  ] =
    getVectors(vectorSchemaRoot)
}
