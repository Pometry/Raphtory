package com.raphtory.spouts

import java.io.BufferedReader
import java.io.File
import java.io.FileReader

import com.raphtory.core.components.Spout.SpoutTrait
import com.raphtory.core.model.communication.StringSpoutGoing
import com.raphtory.spouts.FirehoseSpout.Message.FireHouseDomain
import com.raphtory.spouts.FirehoseSpout.Message.Increase
import com.raphtory.spouts.FirehoseSpout.Message.NextFile
import com.raphtory.spouts.FirehoseSpout.Message.NextLineBlock
import com.typesafe.scalalogging.LazyLogging

import scala.annotation.tailrec
import scala.concurrent.duration._

final case class FirehoseSpout() extends SpoutTrait[FireHouseDomain, StringSpoutGoing] {
  log.info("initialise FirehoseSpout")
  private val directory  = System.getenv().getOrDefault("FILE_SPOUT_DIRECTORY", "/app").trim
  private val fileName   = System.getenv().getOrDefault("FILE_SPOUT_FILENAME", "").trim //gabNetwork500.csv
  private val dropHeader = System.getenv().getOrDefault("FILE_SPOUT_DROP_HEADER", "false").trim.toBoolean
  private val JUMP       = System.getenv().getOrDefault("FILE_SPOUT_BLOCK_SIZE", "10").trim.toInt
  private val INCREMENT  = System.getenv().getOrDefault("FILE_SPOUT_INCREMENT", "1").trim.toInt
  private val TIME       = System.getenv().getOrDefault("FILE_SPOUT_TIME", "60").trim.toInt

  private var fileManager = FileManager(directory, fileName, dropHeader, JUMP)

  def startSpout(): Unit = {
    self ! NextLineBlock
    // todo: wvv not sure why we need to keep increasing
    context.system.scheduler.scheduleOnce(TIME.seconds, self, Increase)
  }

  def handleDomainMessage(message: FireHouseDomain): Unit = message match {
    case Increase =>
      if (fileManager.allCompleted)
        log.info("All files read")
      else {
        fileManager = fileManager.increaseBlockSize(INCREMENT)
        context.system.scheduler.scheduleOnce(TIME.seconds, self, Increase)
      }

    case NextLineBlock =>
      if (fileManager.allCompleted)
        log.info("All files read")
      else {
        val (newFileManager, block) = fileManager.nextLineBlock()
        fileManager = newFileManager
        block.foreach(str => sendTuple(StringSpoutGoing(str)))
        self ! NextLineBlock
      }
    case NextFile =>
      if (fileManager.allCompleted)
        log.info("All files read")
      else {
        fileManager = fileManager.nextFile()
        self ! NextLineBlock
      }
  }
}

object FirehoseSpout {
  object Message {
    sealed trait FireHouseDomain
    case object Increase      extends FireHouseDomain
    case object NextLineBlock extends FireHouseDomain
    case object NextFile      extends FireHouseDomain
  }
}

case class FileManager private (
    currentFileReader: Option[BufferedReader],
    restFiles: List[File],
    dropHeader: Boolean,
    blockSize: Int
) extends LazyLogging {
  def nextFile(): FileManager = this.copy(currentFileReader = None)

  lazy val allCompleted: Boolean = currentFileReader.isEmpty && restFiles.isEmpty

  def increaseBlockSize(inc: Int): FileManager = this.copy(blockSize = blockSize + inc)

  def nextLineBlock(): (FileManager, List[String]) = currentFileReader match {
    case None =>
      restFiles match {
        case Nil => (this, List.empty)
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

  private def readBlockAndIsEnd(reader: BufferedReader): (List[String], Boolean) = {
    @tailrec
    def rec(count: Int, result: List[String]): (List[String], Boolean) =
      if (count > 0) {
        val line = reader.readLine()
        if (line != null)
          rec(count - 1, result :+ line)
        else (result, true)
      } else (result, false)
    rec(blockSize, List.empty)
  }

  private def getFileReader(file: File): BufferedReader = {
    logger.info(s"Reading file ${file.getCanonicalPath}")
    if (dropHeader) {
      val br = new BufferedReader(new FileReader(file))
      br.readLine()
      br
    } else
      new BufferedReader(new FileReader(file))
  }
}

object FileManager extends LazyLogging {
  def apply(dir: String, fileName: String, dropHeader: Boolean, blockSize: Int): FileManager = {
    val filesToRead =
      if (fileName.isEmpty)
        getListOfFiles(dir)
      else {
        val file = new File(dir + "/" + fileName)
        if (file.exists && file.isFile)
          List(file)
        else {
          logger.error(s"File $dir/$fileName does not exist or is not file ")
          List.empty
        }
      }
    FileManager(None, filesToRead, dropHeader, blockSize)
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
