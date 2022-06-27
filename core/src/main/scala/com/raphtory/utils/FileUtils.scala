package com.raphtory.utils

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.io.File
import java.io.FileNotFoundException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import scala.language.postfixOps
import scala.sys.process._
import scala.util.matching.Regex

/** This is a helper class predominantly used by the FileSpout, file based Sinks and example Runners to simplify file handling and downloading of remote datasets.
  * It is included for users creating their own examples or alternative file handling spouts/sinks.
  */
object FileUtils {
  private lazy val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def createOrCleanDirectory(path: String, clean: Boolean = true): File = {
    logger.debug(s"Creating temp folder '$path'.")

    val tempDirectory = new File(path)

    if (tempDirectory.exists()) {
      logger.debug(s"Temporary directory '$tempDirectory' already exists.")

      // If tempDirectory already exists
      // then delete it and recreate it entirely
      if (clean) tempDirectory.delete()
    }
    try {
      val created = tempDirectory.mkdirs()

      if (created)
        logger.debug(s"Temporary directory '$tempDirectory' successfully created.")
    }
    catch {
      case ex: Exception =>
        logger.error(
                s"Failed to create temporary directory '$tempDirectory', error: ${ex.getMessage}."
        )
        throw ex
    }
    tempDirectory
  }

  def validatePath(path: String): Boolean = {
    // check if exists
    try {
      if (!Files.exists(Paths.get(path)))
        throw new FileNotFoundException(s"File '$path' does not exist.")
      if (!Files.isReadable(Paths.get(path)))
        throw new IllegalStateException(s"File '$path' is not readable.'")
    }
    catch {
      case ex: Exception =>
        logger.error(s"File validation failed, error: ${ex.getMessage}.")
        throw ex
    }

    logger.trace(s"File '$path' passed all validation checks.")
    true
  }

  def getMatchingFiles(path: String, regex: Regex, recurse: Boolean): List[File] = {
    val file = new File(path)
    if (file.isFile) {
      logger.debug(s"Found single file ${file.getPath} matching criteria.")
      List(file)
    }
    else if (file.isDirectory) {
      var matchingFiles = file
        .listFiles()
        .filter { file =>
          file.isFile && regex.findFirstIn(file.getName).isDefined
        }

      if (recurse)
        matchingFiles =
          matchingFiles ++ file.listFiles
            .filter(_.isDirectory)
            .flatMap(f => getMatchingFiles(f.getPath, regex, recurse))

      matchingFiles.toList
    }
    else
      throw new IllegalStateException(
              s"Failed to retrieve files. $file is neither a directory nor a file."
      )
  }

  def deleteFile(path: Path): Boolean = {
    try Files.delete(path)
    catch {
      case ex: Exception =>
        logger.error(s"Failed to unlink the file, error: ${ex.getMessage}")
        throw ex
    }
    true
  }

  def curlFile(path: String, url: String) =
    if (!new File(path).exists())
      try s"curl -o $path $url" !!
      catch {
        case ex: Exception =>
          logger.error(s"Failed to download $path due to ${ex.getMessage}.", ex)
          Files.deleteIfExists(Paths.get(path))
          throw ex
      }

}
