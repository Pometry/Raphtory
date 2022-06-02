package com.raphtory.generic

import com.raphtory.BasicGraphBuilder
import com.raphtory.deployment.Raphtory
import com.raphtory.generic.ShutdownTest.signal
import com.raphtory.spouts.SequenceSpout
import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.Semaphore
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.sys.process._

object StreamGraphStopTestRunner extends App {
  val graph = Raphtory.stream(graphBuilder = BasicGraphBuilder(), spout = SequenceSpout("1,1,1"))
  graph.deployment.stop()
  Raphtory.shutdown()
  println(ShutdownTest.signal)
}

object BatchGraphStopTestRunner extends App {
  val graph = Raphtory.load(SequenceSpout("2,2,2"), BasicGraphBuilder())
  graph.deployment.stop()
  Raphtory.shutdown()
  println(ShutdownTest.signal)
}

object ConnectGraphStopTestRunner extends App {
  val graph = Raphtory.connect()
  graph.disconnect()
  Raphtory.shutdown()
  println(ShutdownTest.signal)
}

class ShutdownTest extends AnyFunSuite {
  test("Test that Raphtory stream deployment cleans up and exits")(
          shutdownTest("com.raphtory.generic.StreamGraphStopTestRunner")
  )

  test("Test that Raphtory batch deployment cleans up and exits")(
          shutdownTest("com.raphtory.generic.BatchGraphStopTestRunner")
  )

  test("Test that Raphtory connection cleans up and exits")(
          shutdownTest("com.raphtory.generic.ConnectGraphStopTestRunner")
  )

  def shutdownTest(runner: String): Unit = {
    val lock    = new Semaphore(1)
    var done    = false
    val timeout = 5
    lock.acquire()
    val plogger = ProcessLogger(
            line => {
              println(line)
              //              start the timeout for the test if we see "shutdown complete"
              if (line.contains(signal)) {
                done = true
                println("releasing lock")
                lock.release()
              }
            },
            line => println(line)
    )
    //    Run test as separate process to check shutdown behaviour
    val p       = Seq("sbt", s"core/Test/runMain $runner").run(plogger)
    val f       =
      Future(blocking {
        // make sure to stop the wait if the process completes without printing "shutdown complete" due to errors
        val e = p.exitValue()
        done = true
        lock.release()
        e
      })

    lock.acquire()
    if (!done)
      fail("test logic is broken")
    println("lock released")
    // wait for process to finish with timeout
    try assert(Await.result(f, duration.Duration(timeout, "sec")) == 0)
    catch {
      case _: TimeoutException =>
        p.destroy()
        fail(s"Process failed to terminate after $timeout seconds")
    }
  }
}

object ShutdownTest {
  val signal = "shutdown complete"

}
