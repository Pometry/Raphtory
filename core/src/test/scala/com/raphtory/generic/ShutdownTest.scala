package com.raphtory.generic

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import com.raphtory.BasicGraphBuilder
import com.raphtory.Raphtory
import com.raphtory.generic.ShutdownTest.signal
import com.raphtory.spouts.SequenceSpout
import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.Semaphore
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.sys.process._

object StreamGraphStopTestRunner extends IOApp {

  override def run(args: List[String]): IO[ExitCode] =
    Raphtory
      .quickIOGraph()
      .use(_ => IO(println(ShutdownTest.signal)))
      .map(_ => ExitCode.Success)
}

object ConnectGraphStopTestRunner extends App {
  //TODO: figure out connect
  val graph = Raphtory.connect()
  graph.close()
  println(ShutdownTest.signal)
}

object ShutdownTest {
  val signal = "shutdown complete"

}
