package com.raphtory.dev.wordSemantic.spouts

import java.io.File

import com.raphtory.core.actors.Spout.Spout
import com.typesafe.scalalogging.LazyLogging
import com.github.mjakubowski84.parquet4s.ParquetReader

case class Update(_1: Long, _2: String, _3:String, _4:Long)

class CoMatParquetSpout() extends Spout[Update] {
  private val directory = System.getenv().getOrDefault("FILE_SPOUT_DIRECTORY", "/app").trim
  private val fileName = System.getenv().getOrDefault("FILE_SPOUT_FILENAME", "").trim
  private val dropHeader = System.getenv().getOrDefault("FILE_SPOUT_DROP_HEADER", "false").trim.toBoolean

  private var fileManager = ParquetManager(directory, fileName, dropHeader)

  override def setupDataSource(): Unit = {}

  override def generateData(): Option[Update] =
    if (fileManager.allCompleted) {
      dataSourceComplete()
      None
    }
    else {
      val (newParquetManager, line) = fileManager.nextLine()
      fileManager = newParquetManager
      Option(line)
    }

  override def closeDataSource(): Unit = {}

}

final case class ParquetManager private (
                                          currentFileReader: Option[Iterator[Update]],
                                          restFiles: List[File],
                                          dropHeader: Boolean
                                        ) extends LazyLogging {
  def nextFile(): ParquetManager = this.copy(currentFileReader = None)

  lazy val allCompleted: Boolean = currentFileReader.isEmpty && restFiles.isEmpty


  def nextLine(): (ParquetManager, Update) = currentFileReader match {
    case None =>
      restFiles match {
        case Nil => (this, null)
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

  private def readBlockAndIsEnd(reader: Iterator[Update]): (Update, Boolean) = {
    if (reader.hasNext)
      (reader.next(), false)
    else{
      (null ,true)
    }
  }

  private def getFileReader(file: File): Iterator[Update] = {
    println(s"Reading file ${file.getCanonicalPath}")
    val br = ParquetReader.read[Update](file.getCanonicalPath)
    br.toIterator
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
