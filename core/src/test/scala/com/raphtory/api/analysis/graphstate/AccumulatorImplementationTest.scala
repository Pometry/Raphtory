package com.raphtory.api.analysis.graphstate

import munit.FunSuite

import scala.collection.parallel.CollectionConverters._

class AccumulatorImplementationTest extends FunSuite {

  test("bad accumulator will eventually fail") {
    val n   = 500_000
    val accMatches = (0 until 10).map{ _ =>
      var acc = 0
      (1 to n).par.foreach { i =>
        acc += i
      }
      val exp = (n * (n + 1)) / 2
      acc == exp
    }.reduce(_ && _)

   assert(!accMatches)
  }

  test("int accumulator over multiple threads should be the same as sequential") {
    val n   = 500_000
    val acc = new IntAccumulatorImpl(0, false, _ + _)
    val exp = (n * (n + 1)) / 2

    accumulatorCheck(acc, (1 to n), exp)
  }

  test("long accumulator over multiple threads should be the same as sequential") {
    val n: Long                              = 10_000_000L
    val acc: ConcurrentAccumulatorImpl[Long] = new ConcurrentAccumulatorImpl[Long](0L, false, _ + _)
    val exp: Long                            = (n * (n + 1L)) / 2L

    accumulatorCheck(acc, (1L to n), exp)
    acc.reset()
    assertEquals(acc.value, exp)
    assertEquals(acc.currentValue, 0L)
  }

  test("histogram counter can sum up things and return top 5 elems") {
    val c = new CounterImplementation[String]

    def powers(of: Int) =
      (0 to 10).map(Math.pow(of, _)).map(_.toInt)

    val pow2 = powers(2)
    val pow3 = powers(3)
    val pow4 = powers(4)

    Set("2" -> pow2, "3" -> pow3, "4" -> pow4).foreach {
      case (tag, pow) =>
        pow.foldLeft(c) { (c, i) =>
          c.+=(tag, i)
          c
        }
    }

    val (tag, count) = c.largest

    assertEquals(tag, "4")
    assertEquals(count, pow4.sum)

    val top2 = c.largest(2)
    assertEquals(top2.toList, List("3" -> pow3.sum, "4" -> pow4.sum))

  }

  private def accumulatorCheck[T](acc: AccumulatorImplementation[T, T], is: Iterable[T], exp: T): Unit = {
    is.par.foreach { i =>
      acc += i
    }
    assert(acc.currentValue == exp)
  }
}
