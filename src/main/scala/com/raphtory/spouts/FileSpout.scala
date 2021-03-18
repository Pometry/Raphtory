package com.raphtory.spouts

import java.io._
import java.util.zip.GZIPInputStream

import com.raphtory.core.actors.Spout.Spout
import com.typesafe.scalalogging.LazyLogging

class FileSpout extends Spout[String] {
  //TODO work out loggging here
  //log.info("initialise FileSpout")
  private val directory = System.getenv().getOrDefault("FILE_SPOUT_DIRECTORY", "/app").trim
  private val fileName = System.getenv().getOrDefault("FILE_SPOUT_FILENAME", "").trim //gabNetwork500.csv
  private val dropHeader = System.getenv().getOrDefault("FILE_SPOUT_DROP_HEADER", "false").trim.toBoolean
  //  private val JUMP       = System.getenv().getOrDefault("FILE_SPOUT_BLOCK_SIZE", "10").trim.toInt
  private val INCREMENT = System.getenv().getOrDefault("FILE_SPOUT_INCREMENT", "0").trim.toInt
  private val TIME = System.getenv().getOrDefault("FILE_SPOUT_TIME", "60").trim.toInt

  private var fileManager = FileManager(directory, fileName, dropHeader)

  override def generateData(): Option[String] = {
    if (fileManager.allCompleted) {
      dataSourceComplete()
      None
    }
    else {
      val (newFileManager, line) = fileManager.nextLine()
      fileManager = newFileManager
      if (line.isEmpty ) None else Some(line)
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
//    logger.info(s"Reading file ${file.getCanonicalPath}")
    println(s"Reading file ${file.getCanonicalPath}")
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
    if (d.exists && d.isDirectory) {
      val files = getRecursiveListOfFiles(d)
      files.filter(f => f.isFile && !f.isHidden)
    }
    else {
      println(s"Directory $dir does not exist or is not directory")
      List.empty
    }
  }

  private def getRecursiveListOfFiles(dir: File): List[File] = {
    val these = dir.listFiles.toList
    these ++ these.filter(_.isDirectory).flatMap(getRecursiveListOfFiles)
  }
}
