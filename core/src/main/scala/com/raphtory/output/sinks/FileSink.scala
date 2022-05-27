package com.raphtory.output.sinks

import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter

class FileSink(filePath: String, jobID: String, partitionID: Int, entityDelimiter: String = "\n")
        extends AbstractStreamSink(entityDelimiter) {
  private val workDirectory      = s"$filePath/$jobID"
  new File(workDirectory).mkdirs()
  private val fileWriter         = new FileWriter(s"$workDirectory/partition-$partitionID")
  private val bufferedFileWriter = new BufferedWriter(fileWriter)

  override def write(value: String): Unit =
    bufferedFileWriter.write(value)

  override def close(): Unit = {
    bufferedFileWriter.close()
    fileWriter.close()
  }
}
