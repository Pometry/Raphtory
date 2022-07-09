package com.raphtory.formats

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.raphtory.Raphtory
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.output.format.Format
import com.raphtory.api.time.DiscreteInterval
import com.raphtory.internals.graph.Perspective
import munit.FunSuite

class JsonFormatTest extends FunSuite {
  private val jobID       = "job-id"
  private val partitionID = 13
  private val reader      = JsonMapper.builder().addModule(DefaultScalaModule).build().reader()

  private val sampleTable = List(
          (Perspective(100, None, 0, 100), List(Row("id1", 34), Row("id2", 24))),
          (
                  Perspective(200, Some(DiscreteInterval(200)), 0, 200),
                  List(Row("id1", 56), Row("id2", 67))
          )
  )

  private val rowLevelOutput =
    """{"timestamp":100,"window":null,"row":["id1",34]}
      |{"timestamp":100,"window":null,"row":["id2",24]}
      |{"timestamp":200,"window":200,"row":["id1",56]}
      |{"timestamp":200,"window":200,"row":["id2",67]}
      |""".stripMargin

  private val globalLevelOutput =
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
    val output = formatTable(JsonFormat(), sampleTable, jobID, partitionID)
    assertEquals(output, rowLevelOutput)
  }

  test("JsonFormat.ROW level output lines are valid JSON") {
    val output = formatTable(JsonFormat(), sampleTable, jobID, partitionID)
    output.split("\n") foreach { line =>
      reader.createParser(line).readValueAs(classOf[Any])
    }
  }

  test("JsonFormat.GLOBAL level output matches table data") {
    val output = formatTable(JsonFormat(JsonFormat.GLOBAL), sampleTable, jobID, partitionID)
    assertEquals(output, globalLevelOutput)
  }

  test("JsonFormat.GLOBAL level output is valid JSON") {
    val output = formatTable(JsonFormat(JsonFormat.GLOBAL), sampleTable, jobID, partitionID)
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

  private def formatTable(
      format: Format,
      table: List[(Perspective, List[Row])],
      jobID: String,
      partitionID: Int
  ): String = {
    val sink     = StringSink(format = format)
    val executor = sink.executor(jobID, partitionID, Raphtory.getDefaultConfig())

    table foreach {
      case (perspective, rows) =>
        executor.setupPerspective(perspective)
        rows foreach (row => executor.threadSafeWriteRow(row))
        executor.closePerspective()
    }
    executor.close()

    sink.output
  }
}
