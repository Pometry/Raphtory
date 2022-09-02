package com.raphtory.internals.communication.models.graphalterations

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo._
import org.apache.arrow.vector.VectorSchemaRoot
import com.raphtory.arrowmessaging.model._
import com.raphtory.arrowmessaging.shapelessarrow._
import com.raphtory.internals.graph.GraphAlteration._
import org.apache.arrow.vector.complex.ListVector
import com.raphtory.internals.communication.SchemaProviderInstances._
import scala.reflect.ClassTag

case class SyncExistingRemovalsArrowFlightMessage(
    sourceID: Long = 0L,
    updateTime: Long = 0L,
    index: Long = 0L,
    srcId: Long = 0L,
    dstId: Long = 0L,
    removals1: List[Long] = List.empty[Long],
    removals2: List[Long] = List.empty[Long],
    fromAddition: Boolean = false
) extends ArrowFlightMessage

case class SyncExistingRemovalsArrowFlightMessageVectors(
    sourceIDs: BigIntVector,
    updateTimes: BigIntVector,
    indexes: BigIntVector,
    srcIds: BigIntVector,
    dstIds: BigIntVector,
    removals1: ListVector,
    removals2: ListVector,
    fromAdditions: BitVector
) extends ArrowFlightMessageVectors

case class SyncExistingRemovalsArrowFlightMessageSchema[
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
    val msg = getMessageAtRow(row).asInstanceOf[SyncExistingRemovalsArrowFlightMessage]
    SyncExistingRemovals(
            msg.sourceID,
            msg.updateTime,
            msg.index,
            msg.srcId,
            msg.dstId,
            msg.removals1 zip msg.removals2,
            msg.fromAddition
    )
      .asInstanceOf[T]
  }

  override def encodeMessage[T](msg: T): ArrowFlightMessage = {
    val fmsg     = msg.asInstanceOf[SyncExistingRemovals]
    val (s1, s2) = fmsg.removals.unzip
    SyncExistingRemovalsArrowFlightMessage(
            fmsg.sourceID,
            fmsg.updateTime,
            fmsg.index,
            fmsg.srcId,
            fmsg.dstId,
            s1,
            s2,
            fmsg.fromAddition
    )
  }
}

class SyncExistingRemovalsArrowFlightMessageSchemaFactory extends ArrowFlightMessageSchemaFactory {

  private def getVectors(
      vectorSchemaRoot: VectorSchemaRoot
  ): SyncExistingRemovalsArrowFlightMessageSchema[
          SyncExistingRemovalsArrowFlightMessageVectors,
          SyncExistingRemovalsArrowFlightMessage
  ] = {
    val sourceIDs     = vectorSchemaRoot.getVector("sourceIDs").asInstanceOf[BigIntVector]
    val updateTimes   = vectorSchemaRoot.getVector("updateTimes").asInstanceOf[BigIntVector]
    val indexes       = vectorSchemaRoot.getVector("indexes").asInstanceOf[BigIntVector]
    val srcIds        = vectorSchemaRoot.getVector("srcIds").asInstanceOf[BigIntVector]
    val dstIds        = vectorSchemaRoot.getVector("dstIds").asInstanceOf[BigIntVector]
    val removals1     = vectorSchemaRoot.getVector("removals1").asInstanceOf[ListVector]
    val removals2     = vectorSchemaRoot.getVector("removals2").asInstanceOf[ListVector]
    val fromAdditions = vectorSchemaRoot.getVector("fromAdditions").asInstanceOf[BitVector]

    SyncExistingRemovalsArrowFlightMessageSchema(
            vectorSchemaRoot,
            SyncExistingRemovalsArrowFlightMessageVectors(
                    sourceIDs,
                    updateTimes,
                    indexes,
                    srcIds,
                    dstIds,
                    removals1,
                    removals2,
                    fromAdditions
            )
    )
  }

  override def getInstance(
      allocator: BufferAllocator
  ): SyncExistingRemovalsArrowFlightMessageSchema[
          SyncExistingRemovalsArrowFlightMessageVectors,
          SyncExistingRemovalsArrowFlightMessage
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
                              "removals1",
                              FieldType.notNullable(ArrowType.List.INSTANCE),
                              List(
                                      new Field(
                                              "removalelems1",
                                              FieldType.notNullable(new ArrowType.Int(64, true)),
                                              null
                                      )
                              ).asJava
                      ),
                      new Field(
                              "removals2",
                              FieldType.notNullable(ArrowType.List.INSTANCE),
                              List(
                                      new Field(
                                              "removalelems2",
                                              FieldType.notNullable(new ArrowType.Int(64, true)),
                                              null
                                      )
                              ).asJava
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
  ): SyncExistingRemovalsArrowFlightMessageSchema[
          SyncExistingRemovalsArrowFlightMessageVectors,
          SyncExistingRemovalsArrowFlightMessage
  ] =
    getVectors(vectorSchemaRoot)
}
