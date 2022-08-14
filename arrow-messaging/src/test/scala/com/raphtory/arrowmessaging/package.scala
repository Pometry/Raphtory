package com.raphtory.arrowmessaging

import com.raphtory.arrowmessaging.arrowmessaging.getValueVectors
import com.raphtory.arrowmessaging.arrowmessaging.schema
import com.raphtory.arrowmessaging.model._
import com.raphtory.arrowmessaging.shapelessarrow._
import org.apache.arrow.memory._
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.types._
import org.apache.arrow.vector.types.pojo._

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

package object arrowmessaging {
  lazy val allocator: BufferAllocator = new RootAllocator()

  lazy val schema: Schema =
    new Schema(
            List(
                    new Field("int", new FieldType(false, new ArrowType.Int(32, true), null), null),
                    new Field(
                            "float",
                            new FieldType(false, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null),
                            null
                    ),
                    new Field(
                            "double",
                            new FieldType(false, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null),
                            null
                    ),
                    new Field("long", new FieldType(false, new ArrowType.Int(64, true), null), null),
                    new Field("str", new FieldType(false, new ArrowType.Utf8(), null), null),
                    new Field("bool", new FieldType(false, new ArrowType.Bool(), null), null),
                    new Field("char", new FieldType(false, new ArrowType.Utf8(), null), null),
                    new Field(
                            "intset",
                            FieldType.notNullable(ArrowType.List.INSTANCE),
                            List(new Field("intelems", FieldType.notNullable(new ArrowType.Int(32, true)), null)).asJava
                    ),
                    new Field(
                            "strset",
                            FieldType.notNullable(ArrowType.List.INSTANCE),
                            List(new Field("strelems", FieldType.notNullable(ArrowType.Utf8.INSTANCE), null)).asJava
                    ),
                    new Field(
                            "intlist",
                            FieldType.notNullable(ArrowType.List.INSTANCE),
                            List(new Field("intelems", FieldType.notNullable(new ArrowType.Int(32, true)), null)).asJava
                    ),
                    new Field(
                            "strlist",
                            FieldType.notNullable(ArrowType.List.INSTANCE),
                            List(new Field("strelems", FieldType.notNullable(ArrowType.Utf8.INSTANCE), null)).asJava
                    ),
                    new Field(
                            "longlist",
                            FieldType.notNullable(ArrowType.List.INSTANCE),
                            List(new Field("strelems", FieldType.notNullable(new ArrowType.Int(64, true)), null)).asJava
                    ),
                    new Field(
                            "doublelist",
                            FieldType.notNullable(ArrowType.List.INSTANCE),
                            List(
                                    new Field(
                                            "strelems",
                                            FieldType.notNullable(
                                                    new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
                                            ),
                                            null
                                    )
                            ).asJava
                    ),
                    new Field(
                            "floatlist",
                            FieldType.notNullable(ArrowType.List.INSTANCE),
                            List(
                                    new Field(
                                            "strelems",
                                            FieldType.notNullable(
                                                    new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
                                            ),
                                            null
                                    )
                            ).asJava
                    )
            ).asJava
    )

  def getValueVectors(vectorSchemaRoot: VectorSchemaRoot) = {
    val ints       = vectorSchemaRoot.getVector("int").asInstanceOf[IntVector]
    val floats     = vectorSchemaRoot.getVector("float").asInstanceOf[Float4Vector]
    val doubles    = vectorSchemaRoot.getVector("double").asInstanceOf[Float8Vector]
    val longs      = vectorSchemaRoot.getVector("long").asInstanceOf[BigIntVector]
    val strs       = vectorSchemaRoot.getVector("str").asInstanceOf[VarCharVector]
    val bools      = vectorSchemaRoot.getVector("bool").asInstanceOf[BitVector]
    val chars      = vectorSchemaRoot.getVector("char").asInstanceOf[VarCharVector]
    val intset     = vectorSchemaRoot.getVector("intset").asInstanceOf[ListVector]
    val strset     = vectorSchemaRoot.getVector("strset").asInstanceOf[ListVector]
    val intlist    = vectorSchemaRoot.getVector("intlist").asInstanceOf[ListVector]
    val strlist    = vectorSchemaRoot.getVector("strlist").asInstanceOf[ListVector]
    val longlist   = vectorSchemaRoot.getVector("longlist").asInstanceOf[ListVector]
    val doublelist = vectorSchemaRoot.getVector("doublelist").asInstanceOf[ListVector]
    val floatlist  = vectorSchemaRoot.getVector("floatlist").asInstanceOf[ListVector]

    (
            ints,
            floats,
            doubles,
            longs,
            strs,
            bools,
            chars,
            intset,
            strset,
            intlist,
            strlist,
            longlist,
            doublelist,
            floatlist
    )
  }

  val mixMessage =
    MixArrowFlightMessage(
            900,
            123f,
            888d,
            2000L,
            "One",
            true,
            'S',
            Set(1, 2, 3),
            Set("Pometry", "Raphtory", "UK"),
            List(1, 2, 3),
            List("Pometry", "Raphtory", "UK"),
            List(10L, 20L, 30L),
            List(11d, 12d, 13d),
            List(21f, 22f, 23f)
    )
}

case class FakeMessage()

case class MixArrowFlightMessage(
    int: Int = 0,
    float: Float = 0f,
    double: Double = 0d,
    long: Long = 0L,
    str: String = "",
    bool: Boolean = false,
    char: Char = ' ',
    intset: Set[Int] = Set.empty[Int],
    strset: Set[String] = Set.empty[String],
    intlist: List[Int] = List.empty[Int],
    strlist: List[String] = List.empty[String],
    longlist: List[Long] = List.empty[Long],
    doublelist: List[Double] = List.empty[Double],
    floatlist: List[Float] = List.empty[Float]
) extends ArrowFlightMessage

case class MixArrowFlightMessageVectors(
    ints: IntVector,
    floats: Float4Vector,
    doubles: Float8Vector,
    longs: BigIntVector,
    strs: VarCharVector,
    bools: BitVector,
    chars: VarCharVector,
    intset: ListVector,
    strset: ListVector,
    intlist: ListVector,
    strlist: ListVector,
    longlist: ListVector,
    doublelist: ListVector,
    floatlist: ListVector
) extends ArrowFlightMessageVectors

case class MixArrowFlightMessageSchema[A <: ArrowFlightMessageVectors, B <: ArrowFlightMessage] private (
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
    val msg = getMessageAtRow(row).asInstanceOf[MixArrowFlightMessage]
    msg.asInstanceOf[T]
  }

  override def encodeMessage[T](msg: T): ArrowFlightMessage = msg.asInstanceOf[ArrowFlightMessage]
}

class MixArrowFlightMessageSchemaFactory extends ArrowFlightMessageSchemaFactory {

  override def getInstance(
      allocator: BufferAllocator
  ): MixArrowFlightMessageSchema[MixArrowFlightMessageVectors, MixArrowFlightMessage] = {

    val vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)
    val (
            ints,
            floats,
            doubles,
            longs,
            strs,
            bools,
            chars,
            intset,
            strset,
            intlist,
            strlist,
            longlist,
            doublelist,
            floatlist
    )                    = getValueVectors(vectorSchemaRoot)

    MixArrowFlightMessageSchema(
            vectorSchemaRoot,
            MixArrowFlightMessageVectors(
                    ints,
                    floats,
                    doubles,
                    longs,
                    strs,
                    bools,
                    chars,
                    intset,
                    strset,
                    intlist,
                    strlist,
                    longlist,
                    doublelist,
                    floatlist
            )
    )
  }

  override def getInstance(
      vectorSchemaRoot: VectorSchemaRoot
  ): MixArrowFlightMessageSchema[MixArrowFlightMessageVectors, MixArrowFlightMessage] = {

    val (
            ints,
            floats,
            doubles,
            longs,
            strs,
            bools,
            chars,
            intset,
            strset,
            intlist,
            strlist,
            longlist,
            doublelist,
            floatlist
    ) = getValueVectors(vectorSchemaRoot)

    MixArrowFlightMessageSchema(
            vectorSchemaRoot,
            MixArrowFlightMessageVectors(
                    ints,
                    floats,
                    doubles,
                    longs,
                    strs,
                    bools,
                    chars,
                    intset,
                    strset,
                    intlist,
                    strlist,
                    longlist,
                    doublelist,
                    floatlist
            )
    )
  }
}
