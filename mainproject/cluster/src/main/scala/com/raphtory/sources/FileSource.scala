package com.raphtory.sources

import java.io.{BufferedReader, File, FileInputStream, FileReader, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util.zip.GZIPInputStream

import com.raphtory.core.components.Spout.{DataSource, DataSourceComplete, NoDataAvailable, Spout}
import com.raphtory.core.model.communication.StringSpoutGoing
import com.typesafe.scalalogging.LazyLogging

import scala.annotation.tailrec
import scala.concurrent.duration._

class FileSpout extends DataSource {
  //TODO work out loggging here
  //log.info("initialise FileSpout")
  private val directory = System.getenv().getOrDefault("FILE_SPOUT_DIRECTORY", "/app").trim
  private val fileName = System.getenv().getOrDefault("FILE_SPOUT_FILENAME", "").trim //gabNetwork500.csv
  private val dropHeader = System.getenv().getOrDefault("FILE_SPOUT_DROP_HEADER", "false").trim.toBoolean
  //  private val JUMP       = System.getenv().getOrDefault("FILE_SPOUT_BLOCK_SIZE", "10").trim.toInt
  private val INCREMENT = System.getenv().getOrDefault("FILE_SPOUT_INCREMENT", "0").trim.toInt
  private val TIME = System.getenv().getOrDefault("FILE_SPOUT_TIME", "60").trim.toInt

  private var fileManager = FileManager(directory, fileName, dropHeader)

  override def generateData(): StringSpoutGoing = {
    if (fileManager.allCompleted) {
      throw new DataSourceComplete()
    }
    else {
      val (newFileManager, line) = fileManager.nextLine()
      fileManager = newFileManager
      StringSpoutGoing(line)
    }
  }

  override def setupDataSource(): Unit = {}

  override def closeDataSource(): Unit = {}
}

final case class FileManager private (
    currentFileReader: Option[BufferedReader],
    restFiles: List[File],
    dropHeader: Boolean
) extends LazyLogging {
  def nextFile(): FileManager = this.copy(currentFileReader = None)

  lazy val allCompleted: Boolean = currentFileReader.isEmpty && restFiles.isEmpty

  def nextLine(): (FileManager, String) = currentFileReader match {
    case None =>
      restFiles match {
        case Nil => (this, "")
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

  private def readBlockAndIsEnd(reader: BufferedReader): (String, Boolean) = {
    val line = reader.readLine()
    if (line != null)
      (line,false)
    else{
      reader.close()
      ("",true)
    }
  }

  private def getFileReader(file: File): BufferedReader = {
    logger.info(s"Reading file ${file.getCanonicalPath}")

    var br = new BufferedReader(new FileReader(file))
    if (file.getName.endsWith(".gz")) {
      val inStream = new FileInputStream(file)
      val inGzipStream = new GZIPInputStream(inStream)
      val inReader = new InputStreamReader(inGzipStream) //default to UTF-8
      br = new BufferedReader(inReader)
    }
    if (dropHeader) {
      br.readLine()
    }
    br
  }
}

object FileManager extends LazyLogging {
  private val joiner     = System.getenv().getOrDefault("FILE_SPOUT_JOINER", "/").trim //gabNetwork500.csv
  def apply(dir: String, fileName: String, dropHeader: Boolean): FileManager = {
    val filesToRead =
      if (fileName.isEmpty)
        getListOfFiles(dir)
      else {
        val file = new File(dir + joiner + fileName)
        if (file.exists && file.isFile)
          List(file)
        else {
          logger.error(s"File $dir$joiner$fileName does not exist or is not file ")
          List.empty
        }
      }
    FileManager(None, filesToRead, dropHeader)
  }

  private def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory)
      d.listFiles.toList.filter(f => f.isFile && !f.isHidden)
    else {
      logger.error(s"Directory $dir does not exist or is not directory")
      List.empty
    }
  }
}
