package com.raphtory.formats

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.raphtory.Raphtory
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.output.format.Format
import com.raphtory.api.time.DiscreteInterval
import com.raphtory.internals.graph.Perspective
import munit.FunSuite

class JsonFormatTest extends FormatTest {
  private val reader = JsonMapper.builder().addModule(DefaultScalaModule).build().reader()

  private val rowLevelCaseClassOutput =
    """{"timestamp":100,"window":null,"row":{"name":"John","age":25}}
      |""".stripMargin

  private val rowLevelRowOutput =
    """{"timestamp":100,"window":null,"row":["id1",34]}
      |{"timestamp":100,"window":null,"row":["id2",24]}
      |{"timestamp":200,"window":200,"row":["id1",56]}
      |{"timestamp":200,"window":200,"row":["id2",67]}
      |""".stripMargin

  private val globalLevelRowOutput =
    """{
      |  "jobID" : "job-id",
      |  "partitionID" : 13,
      |  "perspectives" : [ {
      |    "timestamp" : 100,
      |    "window" : null,
      |    "rows" : [ [ "id1", 34 ], [ "id2", 24 ] ]
      |  }, {
      |    "timestamp" : 200,
      |    "window" : 200,
      |    "rows" : [ [ "id1", 56 ], [ "id2", 67 ] ]
      |  } ]
      |}
      |""".stripMargin

  test("JsonFormat.ROW level output matches table data") {
    val output = formatTable(JsonFormat(), rowTable, jobID, partitionID)
    assertEquals(output, rowLevelRowOutput)
  }

  test("JsonFormat.ROW level output lines are valid JSON") {
    val output = formatTable(JsonFormat(), rowTable, jobID, partitionID)
    output.split("\n") foreach { line =>
      reader.createParser(line).readValueAs(classOf[Any])
    }
  }

  test("Product fields are correctly written out") {
    val output = formatTable(JsonFormat(), caseClassTable, jobID, partitionID)
    assertEquals(output, rowLevelCaseClassOutput)
  }

  test("JsonFormat.GLOBAL level output matches table data") {
    val output = formatTable(JsonFormat(JsonFormat.GLOBAL), rowTable, jobID, partitionID)
    assertEquals(output, globalLevelRowOutput)
  }

  test("JsonFormat.GLOBAL level output is valid JSON") {
    val output = formatTable(JsonFormat(JsonFormat.GLOBAL), rowTable, jobID, partitionID)
    reader.createParser(output).readValueAs(classOf[Any])
  }

  test("Print json example on the docs") {
    val docRows   = List(Row("id1", 12), Row("id2", 13), Row("id3", 24))
    val docsTable = List(
            (Perspective(10, None, 0, 10), docRows)
    )
    val output    = formatTable(JsonFormat(JsonFormat.GLOBAL), docsTable, "EdgeCount", 0)
    reader.createParser(output).readValueAs(classOf[Any])
  }
}
