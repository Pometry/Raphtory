package com.raphtory

import cats.effect.IO
import cats.effect.kernel.Resource
import com.google.common.hash.Hashing
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.io.File
import java.net.URL
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import scala.sys.process._

object TestUtils {
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def manageTestFile(fileDefinition: Option[(String, URL)]): Resource[IO, Any] =
    fileDefinition match {
      case None           => Resource.eval(IO.unit)
      case Some((p, url)) =>
        val path = Paths.get(p)
        Resource.make(IO.blocking(if (Files.notExists(path)) s"curl -o $path $url" !!))(_ =>
          IO.blocking { // this is a bit hacky but it allows us
            Runtime.getRuntime.addShutdownHook(new Thread {
              override def run(): Unit =
                Files.deleteIfExists(path)
            })
          }
        )
    }

  def generateTestHash(outputDirectory: String, jobId: String): String = {
    val results = getResults(outputDirectory, jobId)
    val hash    = resultsHash(results)
    logger.info(s"Generated hash code: '$hash'.")

    hash
  }

  def resultsHash(results: IterableOnce[String]): String =
    Hashing
      .sha256()
      .hashString(results.iterator.toSeq.sorted.mkString, StandardCharsets.UTF_8)
      .toString

  def getResults(outputDirectory: String, jobID: String): Iterator[String] = {
    val files = new File(outputDirectory + "/" + jobID)
      .listFiles()
      .filter(_.isFile)

    files.iterator.flatMap { file =>
      val source = scala.io.Source.fromFile(file)
      try source.getLines().toList
      catch {
        case e: Exception => throw e
      }
      finally source.close()
    }
  }
}
