package com.raphtory.internals.communication.models.graphalterations

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo._
import org.apache.arrow.vector.VectorSchemaRoot
import com.raphtory.arrowmessaging.model._
import com.raphtory.arrowmessaging.shapelessarrow._
import com.raphtory.api.input._
import com.raphtory.internals.graph.GraphAlteration._
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.types.FloatingPointPrecision
import com.raphtory.internals.communication.SchemaProviderInstances._
import scala.collection.mutable
import scala.reflect.ClassTag

case class SyncNewEdgeAddArrowFlightMessage(
    updateTime: Long = 0L,
    index: Long = 0L,
    srcId: Long = 0L,
    dstId: Long = 0L,
    vType: String = "",
    removals1: List[Long] = List.empty[Long],
    removals2: List[Long] = List.empty[Long],
    immutablePropertyKeys: List[String] = List.empty[String],
    immutablePropertyValues: List[String] = List.empty[String],
    stringPropertyKeys: List[String] = List.empty[String],
    stringPropertyValues: List[String] = List.empty[String],
    longPropertyKeys: List[String] = List.empty[String],
    longPropertyValues: List[Long] = List.empty[Long],
    doublePropertykeys: List[String] = List.empty[String],
    doublePropertyValues: List[Double] = List.empty[Double],
    floatProperyKeys: List[String] = List.empty[String],
    floatPropertyValues: List[Float] = List.empty[Float]
) extends ArrowFlightMessage

case class SyncNewEdgeAddArrowFlightMessageVectors(
    updateTimes: BigIntVector,
    indexes: BigIntVector,
    srcIds: BigIntVector,
    dstIds: BigIntVector,
    vTypes: VarCharVector,
    removals1: ListVector,
    removals2: ListVector,
    immutablePropertyKeys: ListVector,
    immutablePropertyValues: ListVector,
    stringPropertyKeys: ListVector,
    stringPropertyValues: ListVector,
    longPropertyKeys: ListVector,
    longPropertyValues: ListVector,
    doublePropertykeys: ListVector,
    doublePropertyValues: ListVector,
    floatProperyKeys: ListVector,
    floatPropertyValues: ListVector
) extends ArrowFlightMessageVectors

case class SyncNewEdgeAddArrowFlightMessageSchema[
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

  override def decodeMessage[T](row: Int): T =
    try {
      val msg           = getMessageAtRow(row).asInstanceOf[SyncNewEdgeAddArrowFlightMessage]
      val maybeTypeName = if (msg.vType == "") None else Some(Type(msg.vType))

      val immutableProperties =
        (msg.immutablePropertyKeys zip msg.immutablePropertyValues).map {
          case (k, v) => ImmutableProperty(k, v)
        }

      val stringProperties =
        (msg.stringPropertyKeys zip msg.stringPropertyValues).map {
          case (k, v) => StringProperty(k, v)
        }

      val longProperties =
        (msg.longPropertyKeys zip msg.longPropertyValues).map {
          case (k, v) => LongProperty(k, v)
        }

      val doubleProperties =
        (msg.doublePropertykeys zip msg.doublePropertyValues).map {
          case (k, v) => DoubleProperty(k, v)
        }

      val floatProperties =
        (msg.floatProperyKeys zip msg.floatPropertyValues).map {
          case (k, v) => FloatProperty(k, v)
        }

      val props = List(
              immutableProperties,
              stringProperties,
              longProperties,
              doubleProperties,
              floatProperties
      ).flatten

      SyncNewEdgeAdd(
              msg.updateTime,
              msg.index,
              msg.srcId,
              msg.dstId,
              Properties(props: _*),
              msg.removals1 zip msg.removals2,
              maybeTypeName
      ).asInstanceOf[T]
    }
    catch {
      case e: Exception => e.printStackTrace(); throw e
    }

  override def encodeMessage[T](msg: T): ArrowFlightMessage =
    try {
      val vadd     = msg.asInstanceOf[SyncNewEdgeAdd]
      val (s1, s2) = vadd.removals.unzip
      val vType    = if (vadd.vType.isDefined) vadd.vType.get.name else ""

      val immutablePropertyKeys   = mutable.ListBuffer.empty[String]
      val immutablePropertyValues = mutable.ListBuffer.empty[String]
      val stringPropertyKeys      = mutable.ListBuffer.empty[String]
      val stringPropertyValues    = mutable.ListBuffer.empty[String]
      val longPropertyKeys        = mutable.ListBuffer.empty[String]
      val longPropertyValues      = mutable.ListBuffer.empty[Long]
      val doublePropertyKeys      = mutable.ListBuffer.empty[String]
      val doublePropertyValues    = mutable.ListBuffer.empty[Double]
      val floatPropertyKeys       = mutable.ListBuffer.empty[String]
      val floatPropertyValues     = mutable.ListBuffer.empty[Float]

      vadd.properties.properties.foreach {
        case ImmutableProperty(key, value) =>
          immutablePropertyKeys.addOne(key)
          immutablePropertyValues.addOne(value)

        case StringProperty(key, value)    =>
          stringPropertyKeys.addOne(key)
          stringPropertyValues.addOne(value)

        case LongProperty(key, value)      =>
          longPropertyKeys.addOne(key)
          longPropertyValues.addOne(value)

        case DoubleProperty(key, value)    =>
          doublePropertyKeys.addOne(key)
          doublePropertyValues.addOne(value)

        case FloatProperty(key, value)     =>
          floatPropertyKeys.addOne(key)
          floatPropertyValues.addOne(value)
      }

      SyncNewEdgeAddArrowFlightMessage(
              vadd.updateTime,
              vadd.index,
              vadd.srcId,
              vadd.dstId,
              vType,
              s1,
              s2,
              immutablePropertyKeys.toList,
              immutablePropertyValues.toList,
              stringPropertyKeys.toList,
              stringPropertyValues.toList,
              longPropertyKeys.toList,
              longPropertyValues.toList,
              doublePropertyKeys.toList,
              doublePropertyValues.toList,
              floatPropertyKeys.toList,
              floatPropertyValues.toList
      )

    }
    catch {
      case e: Exception => e.printStackTrace(); throw e
    }
}

class SyncNewEdgeAddArrowFlightMessageSchemaFactory extends ArrowFlightMessageSchemaFactory {

  private def getVectors(
      vectorSchemaRoot: VectorSchemaRoot
  ): SyncNewEdgeAddArrowFlightMessageSchema[
          SyncNewEdgeAddArrowFlightMessageVectors,
          SyncNewEdgeAddArrowFlightMessage
  ] = {
    val updateTimes             = vectorSchemaRoot.getVector("updateTimes").asInstanceOf[BigIntVector]
    val indexes                 = vectorSchemaRoot.getVector("indexes").asInstanceOf[BigIntVector]
    val srcIds                  = vectorSchemaRoot.getVector("srcIds").asInstanceOf[BigIntVector]
    val dstIds                  = vectorSchemaRoot.getVector("dstIds").asInstanceOf[BigIntVector]
    val vTypes                  = vectorSchemaRoot.getVector("vTypes").asInstanceOf[VarCharVector]
    val removals1               = vectorSchemaRoot.getVector("removals1").asInstanceOf[ListVector]
    val removals2               = vectorSchemaRoot.getVector("removals2").asInstanceOf[ListVector]
    val immutablePropertyKeys   =
      vectorSchemaRoot.getVector("immutablePropertyKeys").asInstanceOf[ListVector]
    val immutablePropertyValues =
      vectorSchemaRoot.getVector("immutablePropertyValues").asInstanceOf[ListVector]
    val stringPropertyKeys      =
      vectorSchemaRoot.getVector("stringPropertyKeys").asInstanceOf[ListVector]
    val stringPropertyValues    =
      vectorSchemaRoot.getVector("stringPropertyValues").asInstanceOf[ListVector]
    val longPropertyKeys        = vectorSchemaRoot.getVector("longPropertyKeys").asInstanceOf[ListVector]
    val longPropertyValues      =
      vectorSchemaRoot.getVector("longPropertyValues").asInstanceOf[ListVector]
    val doublePropertykeys      =
      vectorSchemaRoot.getVector("doublePropertyKeys").asInstanceOf[ListVector]
    val doublePropertyValues    =
      vectorSchemaRoot.getVector("doublePropertyValues").asInstanceOf[ListVector]
    val floatProperyKeys        = vectorSchemaRoot.getVector("floatPropertyKeys").asInstanceOf[ListVector]
    val floatPropertyValues     =
      vectorSchemaRoot.getVector("floatPropertyValues").asInstanceOf[ListVector]

    SyncNewEdgeAddArrowFlightMessageSchema(
            vectorSchemaRoot,
            SyncNewEdgeAddArrowFlightMessageVectors(
                    updateTimes,
                    indexes,
                    srcIds,
                    dstIds,
                    vTypes,
                    removals1,
                    removals2,
                    immutablePropertyKeys,
                    immutablePropertyValues,
                    stringPropertyKeys,
                    stringPropertyValues,
                    longPropertyKeys,
                    longPropertyValues,
                    doublePropertykeys,
                    doublePropertyValues,
                    floatProperyKeys,
                    floatPropertyValues
            )
    )
  }

  override def getInstance(
      allocator: BufferAllocator
  ): SyncNewEdgeAddArrowFlightMessageSchema[
          SyncNewEdgeAddArrowFlightMessageVectors,
          SyncNewEdgeAddArrowFlightMessage
  ] = {
    import scala.jdk.CollectionConverters._

    val schema: Schema =
      new Schema(
              List(
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
                      new Field("vTypes", new FieldType(false, new ArrowType.Utf8(), null), null),
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
                              "immutablePropertyKeys",
                              FieldType.notNullable(ArrowType.List.INSTANCE),
                              List(
                                      new Field(
                                              "immutablePropertyKeyElems",
                                              FieldType.notNullable(ArrowType.Utf8.INSTANCE),
                                              null
                                      )
                              ).asJava
                      ),
                      new Field(
                              "immutablePropertyValues",
                              FieldType.notNullable(ArrowType.List.INSTANCE),
                              List(
                                      new Field(
                                              "immutablePropertyValueElems",
                                              FieldType.notNullable(ArrowType.Utf8.INSTANCE),
                                              null
                                      )
                              ).asJava
                      ),
                      new Field(
                              "stringPropertyKeys",
                              FieldType.notNullable(ArrowType.List.INSTANCE),
                              List(
                                      new Field(
                                              "stringPropertyKeyElems",
                                              FieldType.notNullable(ArrowType.Utf8.INSTANCE),
                                              null
                                      )
                              ).asJava
                      ),
                      new Field(
                              "stringPropertyValues",
                              FieldType.notNullable(ArrowType.List.INSTANCE),
                              List(
                                      new Field(
                                              "stringPropertyValueElems",
                                              FieldType.notNullable(ArrowType.Utf8.INSTANCE),
                                              null
                                      )
                              ).asJava
                      ),
                      new Field(
                              "longPropertyKeys",
                              FieldType.notNullable(ArrowType.List.INSTANCE),
                              List(
                                      new Field(
                                              "longPropertyKeyElems",
                                              FieldType.notNullable(ArrowType.Utf8.INSTANCE),
                                              null
                                      )
                              ).asJava
                      ),
                      new Field(
                              "longPropertyValues",
                              FieldType.notNullable(ArrowType.List.INSTANCE),
                              List(
                                      new Field(
                                              "longPropertyValueElems",
                                              new FieldType(
                                                      false,
                                                      new ArrowType.Int(64, true),
                                                      null
                                              ),
                                              null
                                      )
                              ).asJava
                      ),
                      new Field(
                              "doublePropertyKeys",
                              FieldType.notNullable(ArrowType.List.INSTANCE),
                              List(
                                      new Field(
                                              "doublePropertyKeyElems",
                                              FieldType.notNullable(ArrowType.Utf8.INSTANCE),
                                              null
                                      )
                              ).asJava
                      ),
                      new Field(
                              "doublePropertyValues",
                              FieldType.notNullable(ArrowType.List.INSTANCE),
                              List(
                                      new Field(
                                              "doublePropertyValueElems",
                                              new FieldType(
                                                      false,
                                                      new ArrowType.FloatingPoint(
                                                              FloatingPointPrecision.DOUBLE
                                                      ),
                                                      null
                                              ),
                                              null
                                      )
                              ).asJava
                      ),
                      new Field(
                              "floatPropertyKeys",
                              FieldType.notNullable(ArrowType.List.INSTANCE),
                              List(
                                      new Field(
                                              "floatPropertyKeyElems",
                                              FieldType.notNullable(ArrowType.Utf8.INSTANCE),
                                              null
                                      )
                              ).asJava
                      ),
                      new Field(
                              "floatPropertyValues",
                              FieldType.notNullable(ArrowType.List.INSTANCE),
                              List(
                                      new Field(
                                              "floatPropertyValueElems",
                                              new FieldType(
                                                      false,
                                                      new ArrowType.FloatingPoint(
                                                              FloatingPointPrecision.SINGLE
                                                      ),
                                                      null
                                              ),
                                              null
                                      )
                              ).asJava
                      )
              ).asJava
      )

    val vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)
    getVectors(vectorSchemaRoot)
  }

  override def getInstance(
      vectorSchemaRoot: VectorSchemaRoot
  ): SyncNewEdgeAddArrowFlightMessageSchema[
          SyncNewEdgeAddArrowFlightMessageVectors,
          SyncNewEdgeAddArrowFlightMessage
  ] =
    getVectors(vectorSchemaRoot)
}
