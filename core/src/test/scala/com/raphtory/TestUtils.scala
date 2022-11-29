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
}
