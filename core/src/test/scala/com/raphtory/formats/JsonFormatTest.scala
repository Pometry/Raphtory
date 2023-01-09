package com.raphtory.formats

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.raphtory.api.analysis.table.{KeyPair, Row}
import com.raphtory.api.output.format.Format
import com.raphtory.api.time.DiscreteInterval
import com.raphtory.internals.graph.Perspective
import com.raphtory.internals.management.GraphConfig.ConfigBuilder
import munit.FunSuite

class JsonFormatTest extends FunSuite {
  private val jobID       = "job-id"
  private val partitionID = 13
  private val reader      = JsonMapper.builder().addModule(DefaultScalaModule).build().reader()

  private val sampleTable = List(
          (Perspective(100, None, 0, 100, -1, formatAsDate = false), List(Row(KeyPair("id1", 34)), Row(KeyPair("id2", 24)))),
          (
                  Perspective(200, Some(DiscreteInterval(200)), 0, 200, -1, formatAsDate = false),
                  List(Row(KeyPair("id1", 56)), Row(KeyPair("id2", 67)))
          )
  )

  private val rowLevelOutput =
    """{"timestamp":100,"row":[34]}
      |{"timestamp":100,"row":[24]}
      |{"timestamp":200,"window":200,"row":[56]}
      |{"timestamp":200,"window":200,"row":[67]}
      |""".stripMargin

  private val globalLevelOutput =
    """{
      |  "jobID" : "job-id",
      |  "partitionID" : 13,
      |  "perspectives" : [ {
      |    "timestamp" : 100,
      |    "rows" : [ [ 34 ], [ 24 ] ]
      |  }, {
      |    "timestamp" : 200,
      |    "window" : 200,
      |    "rows" : [ [ 56 ], [ 67 ] ]
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
    val docRows   = List(Row(KeyPair("id1", 12)), Row(KeyPair("id2", 13)), Row(KeyPair("id3", 24)))
    val docsTable = List((Perspective(10, None, 0, 10, -1, formatAsDate = false), docRows))
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
    val config   = ConfigBuilder.getDefaultConfig
    val executor = sink.executor(
            jobID,
            partitionID,
            config
    )

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
