package com.raphtory.python

import cats.effect.ExitCode
import cats.effect.IO
import com.monovore.decline._
import com.monovore.decline.effect._
import com.raphtory.Raphtory
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.ImmutableProperty
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Type
import com.raphtory.internals.management.Py4JServer
import com.raphtory.spouts.FileSpout

import java.nio.file.Files
import java.nio.file.Path

object RaphtoryLocal
        extends CommandIOApp(
                name = "LocalRaphtory",
                header = "Support for running Raphtory locally for pyraphtory",
                version = "0.1.0"
        ) {

  override def main: Opts[IO[ExitCode]] = {
    val input = Opts
      .option[Path](long = "input-file", short = "f", help = "Input file for FileSpout")
      .validate("input-file does not exist")(Files.exists(_))
    input.map { file =>
      (for {
        graph <- Raphtory.loadIO(spout = FileSpout(file.toString), graphBuilder = new LOTRGraphBuilder())
      } yield ExitCode.Success).use(code => IO.never *> IO.pure(code))
    }
  }

}

sealed trait BuilderType

case object Batch     extends BuilderType
case object Streaming extends BuilderType

class LOTRGraphBuilder extends GraphBuilder[String] {

  override def parseTuple(tuple: String): Unit = {
    //    val line = new String(tuple,"UTF-8")
    val fileLine   = tuple.split(",").map(_.trim)
    val sourceNode = fileLine(0)
    val srcID      = assignID(sourceNode)
    val targetNode = fileLine(1)
    val tarID      = assignID(targetNode)
    val timeStamp  = fileLine(2).toLong

    addVertex(
            timeStamp,
            srcID,
            Properties(ImmutableProperty("name", sourceNode)),
            Type("Character")
    )
    addVertex(
            timeStamp,
            tarID,
            Properties(ImmutableProperty("name", targetNode)),
            Type("Character")
    )
    addEdge(timeStamp, srcID, tarID, Type("Character Co-occurence"))
  }

}
