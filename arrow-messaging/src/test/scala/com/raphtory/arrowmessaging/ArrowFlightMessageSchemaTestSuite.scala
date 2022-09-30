package com.raphtory.arrowmessaging

import com.raphtory.arrowmessaging.arrowmessaging.mixMessage
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import scala.collection.immutable.HashSet
import scala.collection.mutable
import scala.jdk.CollectionConverters._

class ArrowFlightMessageSchemaTestSuite extends AnyFunSuite {
  val allocator: BufferAllocator = new RootAllocator()

  private val factory = new MixArrowFlightMessageSchemaFactory()
  private val schema  = factory.getInstance(allocator)

  private val ints       = schema.vectors.ints
  private val floats     = schema.vectors.floats
  private val doubles    = schema.vectors.doubles
  private val longs      = schema.vectors.longs
  private val strs       = schema.vectors.strs
  private val bools      = schema.vectors.bools
  private val chars      = schema.vectors.chars
  private val intset     = schema.vectors.intset
  private val strset     = schema.vectors.strset
  private val intlist    = schema.vectors.intlist
  private val strlist    = schema.vectors.strlist
  private val longlist   = schema.vectors.longlist
  private val doublelist = schema.vectors.doublelist
  private val floatlist  = schema.vectors.floatlist

  test("AllocateNew type derivative allocates new buffers to value vectors") {
    assert(ints.getValueCapacity == 0)
    assert(floats.getValueCapacity == 0)
    assert(doubles.getValueCapacity == 0)
    assert(longs.getValueCapacity == 0)
    assert(strs.getValueCapacity == 0)
    assert(bools.getValueCapacity == 0)
    assert(chars.getValueCapacity == 0)
    assert(intset.getValueCapacity == 0)
    assert(strset.getValueCapacity == 0)
    assert(intlist.getValueCapacity == 0)
    assert(strlist.getValueCapacity == 0)
    assert(longlist.getValueCapacity == 0)
    assert(doublelist.getValueCapacity == 0)
    assert(floatlist.getValueCapacity == 0)

    schema.allocateNew()

    assert(ints.getValueCapacity != 0)
    assert(floats.getValueCapacity != 0)
    assert(doubles.getValueCapacity != 0)
    assert(longs.getValueCapacity != 0)
    assert(strs.getValueCapacity != 0)
    assert(bools.getValueCapacity != 0)
    assert(chars.getValueCapacity != 0)
    assert(intset.getValueCapacity != 0)
    assert(strset.getValueCapacity != 0)
    assert(intlist.getValueCapacity != 0)
    assert(strlist.getValueCapacity != 0)
    assert(longlist.getValueCapacity != 0)
    assert(doublelist.getValueCapacity != 0)
    assert(floatlist.getValueCapacity != 0)
  }

  test("SetSafe type derivative sets given values against a given row for each value vector") {
    schema.addMessages(mixMessage)

    assert(ints.get(0) == 900)
    assert(floats.get(0) == 123f)
    assert(doubles.get(0) == 888d)
    assert(longs.get(0) == 2000L)
    assert(new String(strs.get(0), StandardCharsets.UTF_8) == "One")
    assert(bools.get(0) == 1)
    assert(chars.get(0)(0).asInstanceOf[Char] == 'S')

    assert(intset.getObject(0).asScala.toSet.asInstanceOf[Set[Int]] == HashSet(1, 2, 3))
    val res = mutable.Set.empty[String]
    val itr = strset.getObject(0).iterator()
    while (itr.hasNext) {
      val o = itr.next().asInstanceOf[org.apache.arrow.vector.util.Text]
      res.add(new String(o.getBytes, StandardCharsets.UTF_8))
    }
    assert(res.toSet == HashSet("Pometry", "Raphtory", "UK"))

    assert(intlist.getObject(0).asScala.toList.asInstanceOf[List[Int]] == List(1, 2, 3))
    val res2 = mutable.ListBuffer.empty[String]
    val itr2 = strlist.getObject(0).iterator()
    while (itr2.hasNext) {
      val o = itr2.next().asInstanceOf[org.apache.arrow.vector.util.Text]
      res2.addOne(new String(o.getBytes, StandardCharsets.UTF_8))
    }
    assert(res2.toList == List("Pometry", "Raphtory", "UK"))

    assert(longlist.getObject(0).asScala.toList.asInstanceOf[List[Long]] == List(10L, 20L, 30L))
    assert(doublelist.getObject(0).asScala.toList.asInstanceOf[List[Double]] == List(11d, 12d, 13d))
    assert(floatlist.getObject(0).asScala.toList.asInstanceOf[List[Float]] == List(21f, 22f, 23f))
  }

  test("SetValueCount type derivative sets given rows count against all value vector value counts") {
    assert(ints.getValueCount == 0)
    assert(floats.getValueCount == 0)
    assert(doubles.getValueCount == 0)
    assert(longs.getValueCount == 0)
    assert(strs.getValueCount == 0)
    assert(bools.getValueCount == 0)
    assert(chars.getValueCount == 0)
    assert(intset.getValueCount == 0)
    assert(strset.getValueCount == 0)
    assert(intlist.getValueCount == 0)
    assert(strlist.getValueCount == 0)
    assert(longlist.getValueCount == 0)
    assert(doublelist.getValueCount == 0)
    assert(floatlist.getValueCount == 0)

    schema.completeAddMessages()

    assert(ints.getValueCount == 1)
    assert(floats.getValueCount == 1)
    assert(doubles.getValueCount == 1)
    assert(longs.getValueCount == 1)
    assert(strs.getValueCount == 1)
    assert(bools.getValueCount == 1)
    assert(chars.getValueCount == 1)
    assert(intset.getValueCount == 1)
    assert(strset.getValueCount == 1)
    assert(intlist.getValueCount == 1)
    assert(strlist.getValueCount == 1)
    assert(longlist.getValueCount == 1)
    assert(doublelist.getValueCount == 1)
    assert(floatlist.getValueCount == 1)
  }

  test("IsSet type derivative validates if the value vectors have any values set for a given row") {
    assert(schema.isMessageExistsAtRow(0))
  }

  test("Get type derivative gets encoded message against a given row from value vectors") {
    val encoded = schema.getMessageAtRow(0)
    assert(encoded == mixMessage)
  }

  test(
          "Get type derivative throws exception when encoded message is fetched for a row for which there is no values set"
  ) {
    assertThrows[IllegalStateException] {
      schema.getMessageAtRow(1)
    }
  }

  test("Close type derivative clears buffer allocation for all value vectors") {
    schema.close()

    assert(ints.getValueCapacity == 0)
    assert(floats.getValueCapacity == 0)
    assert(doubles.getValueCapacity == 0)
    assert(longs.getValueCapacity == 0)
    assert(strs.getValueCapacity == 0)
    assert(bools.getValueCapacity == 0)
    assert(chars.getValueCapacity == 0)
    assert(intset.getValueCapacity == 0)
    assert(strset.getValueCapacity == 0)
    assert(intlist.getValueCapacity == 0)
    assert(strlist.getValueCapacity == 0)
    assert(longlist.getValueCapacity == 0)
    assert(doublelist.getValueCapacity == 0)
    assert(floatlist.getValueCapacity == 0)
  }

}
