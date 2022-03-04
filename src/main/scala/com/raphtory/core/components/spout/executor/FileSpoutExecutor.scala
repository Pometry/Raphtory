package com.raphtory.core.components.spout.executor

import com.raphtory.core.components.spout.SpoutExecutor
import com.raphtory.core.config.PulsarController
import com.raphtory.util.FileUtils
import com.typesafe.config.Config
import monix.execution.Scheduler
import org.apache.pulsar.client.api.Schema

import java.io.File
import java.io.FileInputStream
import java.nio.file._
import java.util.concurrent.TimeUnit
import java.util.zip.GZIPInputStream
import java.util.zip.ZipInputStream
import scala.collection.mutable
import scala.io.Source
import scala.util.matching.Regex

class FileSpoutExecutor[T](
    path: String = "",
    schema: Schema[T],
    lineConverter: String => T,
    conf: Config,
    pulsarController: PulsarController,
    scheduler: Scheduler
) extends SpoutExecutor[T](conf: Config, pulsarController: PulsarController, scheduler) {
  private var linesProcessed: Int                 = 0
  private val completedFiles: mutable.Set[String] = mutable.Set.empty[String]

  private val reReadFiles     = conf.getBoolean("raphtory.spout.file.local.reread")
  private val recurse         = conf.getBoolean("raphtory.spout.file.local.recurse")
  private val regexPattern    = conf.getString("raphtory.spout.file.local.fileFilter")
  private val sourceDirectory = conf.getString("raphtory.spout.file.local.sourceDirectory")
  // TODO HARDLINK wont work on a network share
  private val outputDirectory = conf.getString("raphtory.spout.file.local.outputDirectory")

  private val inputPath = Option(path).filter(_.trim.nonEmpty).getOrElse(sourceDirectory)
  private val fileRegex = new Regex(regexPattern)

  private val producer = pulsarController.toBuildersProducer()

  override def run(): Unit =
    readFiles()

  override def stop(): Unit =
    producer.close()

  def readFiles(): Unit = {
    // Validate that the path exists and is readable
    // Throws exception or logs error in case of failure
    FileUtils.validatePath(inputPath) // TODO Change this to cats.Validated
    val files =
      FileUtils.getMatchingFiles(inputPath, regex = fileRegex, recurse = recurse)

    if (files.nonEmpty) {
      val tempDirectory = FileUtils.createOrCleanDirectory(outputDirectory)

      // Remove any files that has already been processed
      val filesToProcess = files.collect {
        case file if !completedFiles.contains(file.getPath.replace(inputPath, "")) =>
          logger.debug(
                  s"Spout: Found a new file ${file.getPath.replace(inputPath, "")} to process."
          )
          // mimic sub dir structure of files
          val sourceSubFolder = tempDirectory.getPath + file.getParent.replace(inputPath, "")
          FileUtils.createOrCleanDirectory(sourceSubFolder, false)
          // Hard link the files for processing
          logger.debug(s"Spout: Attempting to hard link file '$file'.")
          try Files.createLink(
                  Paths.get(sourceSubFolder + "/" + file.getName),
                  file.toPath
          )
          catch {
            case ex: Exception =>
              logger.error(
                      s"Spout: Failed to hard link file ${file.getPath}, error: ${ex.getMessage}."
              )
              throw ex
          }
      }

      filesToProcess.sorted
        .foreach(path => processFile(path))
      if (filesToProcess.nonEmpty)
        logger.info("Spout: Finished reading all files.")
      else
        logger.debug("Spout: No new files to read.")
    }

    rescheduleFilePoll()
  }

  def sendMessage(file: File, line: String): Unit = {
    val data = lineConverter(line)
    producer.sendAsync(kryo.serialise(data))
  }

  private def processFile(path: Path): Unit = {
    logger.info(s"Spout: Processing file '${path.getFileName}' ...")

    val file = new File(path.toString)

    val fileSize: Long     = file.length
    val percentage: Double = 100.0 / fileSize
    var readLength         = 0
    var progressPercent    = 1

    val fileName = file.getPath.toLowerCase
    val source   = fileName match {
      case name if name.endsWith(".gz")  =>
        Source.fromInputStream(new GZIPInputStream(new FileInputStream(file.getPath)))
      case name if name.endsWith(".zip") =>
        Source.fromInputStream(new ZipInputStream(new FileInputStream(file.getPath)))
      case _                             => Source.fromFile(file)
    }

    try {
      for (line <- source.getLines()) {
        readLength += line.length
        progressPercent = Math.ceil(percentage * readLength).toInt

        sendMessage(file, line)

        linesProcessed = linesProcessed + 1

        if (linesProcessed % 100_000 == 0)
          logger.debug(s"Spout: sent $linesProcessed messages.")
      }

      logger.debug(s"Spout: Finished processing ${file.getName}.")
    }
    catch {
      case ex: Exception =>
        logger.error(s"Spout: Failed to process file, error: ${ex.getMessage}.")
        throw ex
    }
    finally {
      source.close()

      // Add file to tracker so we do not read it again
      if (!reReadFiles) {
        val fileName = file.getPath.replace(outputDirectory, "")
        logger.debug(s"Spout: Adding file $fileName to completed list.")

        completedFiles.add(fileName)
      }

      // Remove hard-link
      FileUtils.deleteFile(file.toPath)
    }
  }

  private def rescheduleFilePoll(): Unit = {
    val runnable = new Runnable {
      override def run(): Unit = readFiles()
    }

    // TODO: Parameterise the delay
    logger.debug("Spout: Scheduling to poll files again in 10 seconds.")
    scheduler.scheduleOnce(10, TimeUnit.SECONDS, runnable)
  }

}
