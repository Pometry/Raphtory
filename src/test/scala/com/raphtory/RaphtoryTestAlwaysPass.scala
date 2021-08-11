package com.raphtory.tests

import org.scalatest.FunSuite

class RaphtoryTestAlwaysPass extends FunSuite {
    test("Pass always test") {
        assert(1 === 1)
    }
    test("Check CSV file exists") {
        assert(new java.io.File("src/test/scala/com/raphtory/data/data1.csv").isFile)
    }
}