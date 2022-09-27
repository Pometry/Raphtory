package test.raphtory

import cats.effect.IO
import cats.effect.SyncIO
import cats.effect.kernel.Resource
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.api.analysis.graphview.TemporalGraph
import com.raphtory.api.input._
import com.raphtory.internals.context.RaphtoryContext
import com.raphtory.lotrtest.LOTRGraphBuilder
import com.raphtory.spouts.FileSpout
import com.raphtory.Raphtory
import com.raphtory.TestUtils
import com.raphtory.api.analysis.table.Row
import com.raphtory.sinks.PrintSink
import munit.CatsEffectSuite
import test.raphtory.algorithms.MaxFlowTest
import test.raphtory.algorithms.MinimalTestAlgorithm
import test.raphtory.algorithms.TestAlgorithmWithExternalDependency

import java.net.URL
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._
import scala.sys.process._
import scala.tools.reflect.ToolBox
import scala.util.Try
import scala.util.Using

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
    Resource.make(IO(remote().newGraph()))(graph => IO(graph.destroy()))

  val remoteProcess: Resource[IO, Process] = Resource.make {
    for {
      process <- IO {
                   println("starting remote process")
                   val classPath =
                     try
                     // It seems like the file below is where sbt actually stores that info so we can just read it directly
                     Using(scala.io.Source.fromFile("core/target/streams/test/fullClasspath/_global/streams/export")) {
                       source =>
                         source.mkString
                     }.get
                     catch {
                       case _: Exception =>
                         // This is how to get the full class path for running core using sbt, however, the command is really slow as it resolves everything from scratch
                         Seq("sbt", "--error", "export core/test:fullClasspath").!!
                     }

                   Process(
                           Seq(
                                   "java",
                                   "-cp",
                                   classPath,
                                   "com.raphtory.service.Standalone"
                           )
                   ).run()
                 }
      _       <- IO.sleep(2.seconds)
    } yield process
  }(process =>
    IO.sleep(1.seconds) *> // chance to get output across
      IO {
        println("destroying remote process")
        process.destroy()
      }
  )

  lazy val remoteGraph = ResourceSuiteLocalFixture(
          "remote-graph",
          remoteGraphResource.evalMap(g => IO(g.addDynamicPath("com.raphtory.lotrtest").load(source())))
  )

  lazy val remoteGraphInline: SyncIO[FunFixture[TemporalGraph]] = ResourceFixture(
          remoteGraphResource
            .evalMap(g => IO(g.addDynamicPath("com.raphtory.examples.lotr")))
            .evalMap(g => IO(g.load(sourceInline())))
  )

  lazy val source: Fixture[Source] = ResourceSuiteLocalFixture[Source](
          "lotr",
          for {
            _      <- TestUtils.manageTestFile(
                              Some("/tmp/lotr.csv", new URL("https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"))
                      )
            source <- Resource.pure(Source(FileSpout("/tmp/lotr.csv"), LOTRGraphBuilder))
          } yield source
  )

  lazy val sourceInline: Fixture[Source] = ResourceSuiteLocalFixture[Source](
          "lotr-inline",
          for {
            _      <- TestUtils.manageTestFile(
                              Some("/tmp/lotr.csv", new URL("https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"))
                      )
            source <- Resource.pure(
                              Source[String](
                                      FileSpout("/tmp/lotr.csv"),
                                      (graph: Graph, tuple: String) => {
                                        val fileLine   = tuple.split(",").map(_.trim)
                                        val sourceNode = fileLine(0)
                                        val srcID      = graph.assignID(sourceNode)
                                        val targetNode = fileLine(1)
                                        val tarID      = graph.assignID(targetNode)
                                        val timeStamp  = fileLine(2).toLong

                                        graph.addVertex(
                                                timeStamp,
                                                srcID,
                                                Properties(ImmutableProperty("name", sourceNode)),
                                                Type("Character")
                                        )
                                        graph.addVertex(
                                                timeStamp,
                                                tarID,
                                                Properties(ImmutableProperty("name", targetNode)),
                                                Type("Character")
                                        )
                                        graph.addEdge(timeStamp, srcID, tarID, Type("Character Co-occurence"))
                                      }
                              )
                      )
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
                              Raphtory.connect()
                            }
                          } { c =>
                            IO {
                              println("closing remote connection")
                              //                              c.close() TODO: this currently falls over if any graphs were closed before
                            }
                          }
          } yield connection
  )

  override def munitFixtures = List(remote, source, localGraph, sourceInline, remoteGraph)

  test("test algorithm locally") {
    val res = localGraph().execute(MaxFlowTest[Int]("Gandalf", "Gandalf")).get().toList
    assert(res.nonEmpty)
  }

  test("test locally compiled algo") {
    val res = localGraph().execute(compiledAlgo).get().toList
    assert(res.nonEmpty)
  }

  test("When an algo crashes and we try to iterate over the result we get an exception") {
    val res = Try(remoteGraph().select(vertex => Row(1)).get().toList)
    assert(res.isFailure)
  }

  test("When an algo crashes waitForJob returns and isJobDone is false") {
    val query = remoteGraph().select(vertex => Row(1)).filter(row => false).writeTo(PrintSink())
    query.waitForJob()
    assert(!query.isJobDone)
  }

  test("test algorithm class injection") {
    val res = remoteGraph().execute(MinimalTestAlgorithm).get().toList
    assert(res.nonEmpty)
  }

  test("test manual dynamic path") {
    val res = remoteGraph().addDynamicPath("dependency").execute(TestAlgorithmWithExternalDependency).get().toList
    assert(res.nonEmpty)
  }

  remoteGraphInline.test("test inline graphbuilder definition") { g =>
    val res = g.execute(EdgeList()).get().toList
    assert(res.nonEmpty)
  }

  test("test algorithm class injection with MaxFlow") {
    val res = remoteGraph().execute(MaxFlowTest[Int]("Gandalf", "Gandalf")).get().toList
    assert(res.nonEmpty)
  }
}
