import cats.effect.kernel.Resource
import cats.effect.IO
import cats.effect.SyncIO
import com.raphtory.Raphtory
import com.raphtory.TestUtils
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.api.analysis.graphview.TemporalGraph
import com.raphtory.api.input.Source
import com.raphtory.examples.lotr.analysis.ArbitraryMessage
import com.raphtory.examples.lotr.analysis.DegreesSeparation
import com.raphtory.examples.lotr.analysis.FlowAdded
import com.raphtory.examples.lotr.analysis.MaxFlowTest
import com.raphtory.examples.lotr.analysis.Message
import com.raphtory.examples.lotr.analysis.MinimalTestAlgorithm
import com.raphtory.examples.lotr.analysis.NewLabel
import com.raphtory.examples.lotr.analysis.Recheck
import com.raphtory.examples.lotr.graphbuilders.LOTRGraphBuilder
import com.raphtory.internals.context.RaphtoryContext
import com.raphtory.spouts.FileSpout
import com.raphtory.utils.FileUtils
import munit.CatsEffectSuite

import java.net.URL
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._
import scala.sys.process._
import scala.tools.reflect.ToolBox

class DynamicClassLoaderTest extends CatsEffectSuite {

  val minimalTestCode: String =
    """
      |import com.raphtory.api.analysis.algorithm.Generic
      |import com.raphtory.api.analysis.graphview.GraphPerspective
      |
      |
      |
      |object MinimalTestAlgorithm extends Generic {
      |  case class ArbitraryMessage()
      |  override def apply(graph: GraphPerspective): graph.Graph =
      |    graph.step(vertex => vertex.messageAllNeighbours(ArbitraryMessage()))
      |}
      |MinimalTestAlgorithm
      |""".stripMargin

  override def munitTimeout: Duration = FiniteDuration(2, "min")

  private val tb: ToolBox[universe.type] = runtimeMirror(getClass.getClassLoader).mkToolBox()

  private val compiledAlgo: Generic = tb.compile(tb.parse(minimalTestCode))().asInstanceOf[Generic]

  val remoteGraphResource =
    Resource.make(IO(remote().newGraph()))(graph => IO(graph.destroy())).evalMap(g => IO(g.load(source())))

  val remoteProcess: Resource[IO, Process] = Resource.make {
    IO {
      println("starting remote process")
      Process(
              Seq(
                      "sbt",
                      "core/runMain com.raphtory.service.Standalone testLOTR"
              )
      ).run()
    }
  }(process =>
    IO.sleep(1.seconds) *> // chance to get output across
      IO {
        println("destroying remote process")
        process.destroy()
      }
  )

  lazy val remoteGraph = ResourceFixture(remoteGraphResource)

  lazy val remoteGraphWithPath: SyncIO[FunFixture[TemporalGraph]] = ResourceFixture(
          remoteGraphResource.evalMap(g => IO(g.addDynamicPath("com.raphtory.examples.lotr")))
  )

  lazy val source: Fixture[Source] = ResourceSuiteLocalFixture[Source](
          "lotr",
          for {
            _      <- TestUtils.manageTestFile(
                              Some("/tmp/lotr.csv", new URL("https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"))
                      )
            source <- Resource.pure(Source(FileSpout("/tmp/lotr.csv"), new LOTRGraphBuilder()))
          } yield source
  )

  lazy val localGraph: Fixture[DeployedTemporalGraph] = ResourceSuiteLocalFixture(
          "local",
          for {
            g <- Raphtory.newIOGraph()
            _  = g.load(source())
          } yield g
  )

  lazy val remote: Fixture[RaphtoryContext] = ResourceSuiteLocalFixture(
          "standalone",
          for {
            _          <- remoteProcess
            connection <- Resource.make {
                            IO {
                              println("connecting to remote")
                              Raphtory.connect("testLOTR")
                            }
                          } { c =>
                            IO {
                              println("closing remote connection")
//                              c.close() TODO: this currently falls over if any graphs were closed before
                            }
                          }
          } yield connection
  )

  override def munitFixtures = List(remote, source, localGraph)

  test("test algorithm locally") {
    val res = localGraph().execute(MaxFlowTest[Int]("Gandalf", "Gandalf")).get().toList
    assert(res.nonEmpty)
    println(res)
  }

  test("test locally compiled algo") {
    val res = localGraph().execute(compiledAlgo).get().toList
    assert(res.nonEmpty)
    println(res)
  }

  remoteGraphWithPath.test("simple algorithm should work") { g =>
    val res = g.execute(DegreesSeparation()).get().toList
    assert(res.nonEmpty)
    println(res)

  }
  remoteGraphWithPath.test("test algorithm class injection") { g =>
    val res = g.execute(MinimalTestAlgorithm).get().toList
    assert(res.nonEmpty)
    println(res)
  }

  remoteGraph.test("test manual class injection with MaxFlow") { gIn =>
    var g: TemporalGraph = gIn
    g = g.addClass(Class.forName("com.raphtory.examples.lotr.analysis.Message"))
    g = g.addClass(classOf[FlowAdded[_, _]])
    g = g.addClass(classOf[Recheck[_]])
    g = g.addClass(classOf[NewLabel[_]])
    val res              = g.execute(MaxFlowTest[Int]("Gandalf", "Gandalf")).get().toList
    println(res)
    assert(res.nonEmpty)
  }

  remoteGraphWithPath.test("test algorithm class injection with MaxFlow") { g =>
    val res = g.execute(MaxFlowTest[Int]("Gandalf", "Gandalf")).get().toList
    println(res)
    assert(res.nonEmpty)
  }
}
