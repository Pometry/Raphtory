package com.raphtory.testCases.wordSemantic.spouts

import java.io.File
import java.util

import com.raphtory.core.actors.Spout.Spout
import com.raphtory.spouts.FileManager.{joiner, logger}
import org.apache.spark.sql.{Row, SparkSession}
import com.typesafe.scalalogging.LazyLogging

class CoMatParquetSpout() extends Spout[Row] {
  private val directory = System.getenv().getOrDefault("FILE_SPOUT_DIRECTORY", "/app").trim
  private val fileName = System.getenv().getOrDefault("FILE_SPOUT_FILENAME", "").trim
  private val dropHeader = System.getenv().getOrDefault("FILE_SPOUT_DROP_HEADER", "false").trim.toBoolean

  private var fileManager = ParquetManager(directory, fileName, dropHeader)

  override def setupDataSource(): Unit = {}

  override def generateData(): Option[Row] =
    if (fileManager.allCompleted) {
      dataSourceComplete()
      None
    }
    else {
      val (newParquetManager, line) = fileManager.nextLine()
      fileManager = newParquetManager
      if (line!= Row.empty) Some(line) else None
    }

  override def closeDataSource(): Unit = {}

}

final case class ParquetManager private (
                                       currentFileReader: Option[util.Iterator[Row]],
                                       restFiles: List[File],
                                       dropHeader: Boolean
                                     ) extends LazyLogging {
  def nextFile(): ParquetManager = this.copy(currentFileReader = None)

  lazy val allCompleted: Boolean = currentFileReader.isEmpty && restFiles.isEmpty
  lazy val spark: SparkSession =   SparkSession.builder().master("local").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  def nextLine(): (ParquetManager, Row) = currentFileReader match {
    case None =>
      restFiles match {
        case Nil => (this, Row.empty)
        case head :: tail =>
          val reader             = getFileReader(head)
          val (block, endOfFile) = readBlockAndIsEnd(reader)
          val currentReader      = if (endOfFile) None else Some(reader)
          (this.copy(currentFileReader = currentReader, restFiles = tail), block)
      }
    case Some(reader) =>
      val (block, endOfFile) = readBlockAndIsEnd(reader)
      if (endOfFile) (this.copy(currentFileReader = None), block)
      else (this, block)

  }

  private def readBlockAndIsEnd(reader: util.Iterator[Row]): (Row, Boolean) = {
    if (reader.hasNext)
      (reader.next(), false)
    else{
      (Row.empty ,true)
    }
  }

  private def getFileReader(file: File): util.Iterator[Row] = {
    println(s"Reading file ${file.getCanonicalPath}")
    val br = spark.read.parquet(file.getCanonicalPath)
    br.toLocalIterator()
  }
}

object ParquetManager extends LazyLogging {
  private val joiner     = System.getenv().getOrDefault("FILE_SPOUT_JOINER", "/").trim //gabNetwork500.csv
  def apply(dir: String, fileName: String, dropHeader: Boolean): ParquetManager = {
    val filesToRead =
      if (fileName.isEmpty)
        getListOfFiles(dir)
      else {
        val file = new File(dir + joiner + fileName)
        if (file.exists && file.isFile)
          List(file)
        else {
          println(s"File $dir$joiner$fileName does not exist or is not file ")
          List.empty
        }
      }
    ParquetManager(None, filesToRead, dropHeader)
  }

  private def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      val files = getRecursiveListOfFiles(d)
      files.filter(f => f.isFile && !f.isHidden && f.getName.endsWith(".parquet"))
    }
    else {
      logger.error(s"Directory $dir does not exist or is not directory")
      List.empty
    }
  }

  private def getRecursiveListOfFiles(dir: File): List[File] = {
    val these = dir.listFiles.toList
    these ++ these.filter(_.isDirectory).sorted.flatMap(getRecursiveListOfFiles)
  }
}
