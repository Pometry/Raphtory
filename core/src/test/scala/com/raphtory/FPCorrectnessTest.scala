package com.raphtory

abstract class FPCorrectnessTest(compareAsDoubleIndex: Set[Int] = Set(2), tol: Double = 1e-8)
        extends BaseCorrectnessTest {

  override def assertResultsMatch(obtained: IterableOnce[String], results: IterableOnce[String]): Unit =
    obtained.iterator.toSeq.sorted.zip(results.iterator.toSeq.sorted).foreach {
      case (res, expected) =>
        val resSplit      = res.split(",")
        val expectedSplit = expected.split(",")
        for (i <- 0 until resSplit.size)
          if (compareAsDoubleIndex contains i)
            assertEqualsDouble(resSplit(i).toDouble, expectedSplit(i).toDouble, tol)
          else
            assertEquals(resSplit(i), expectedSplit(i))
    }
}
