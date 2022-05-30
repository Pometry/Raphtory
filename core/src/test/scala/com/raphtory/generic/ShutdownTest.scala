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

object GraphStopTestRunner extends App {
  val graph1 = Raphtory.stream(graphBuilder = BasicGraphBuilder(), spout = SequenceSpout("1,1,1"))
  graph1.deployment.stop()
  val graph2 = Raphtory.load(SequenceSpout("2,2,2"), BasicGraphBuilder())
  graph2.deployment.stop()
  Raphtory.shutdown()
  println(ShutdownTest.signal)
}

class ShutdownTest extends AnyFunSuite {
  test("Test that Raphtory deployment cleans up and exits") {
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
    val p       = Seq("sbt", "core/Test/runMain com.raphtory.generic.GraphStopTestRunner").run(plogger)
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
