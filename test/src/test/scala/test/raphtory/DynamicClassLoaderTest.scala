package test.raphtory

import cats.effect.IO
import cats.effect.SyncIO
import cats.effect.kernel.Resource
import cats.effect.std.Semaphore
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.api.analysis.graphview.TemporalGraph
import com.raphtory.api.input._
import com.raphtory.internals.context.RaphtoryContext
import com.raphtory.lotrtest.LOTRGraphBuilder
import com.raphtory.internals.context.RemoteContext
import com.raphtory.spouts.FileSpout
import com.raphtory.Raphtory
import com.raphtory.TestUtils
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.input.sources.CSVEdgeListSource
import com.raphtory.sinks.PrintSink
import com.typesafe.scalalogging.Logger
import com.raphtory.api.input.sources.CSVEdgeListSource
import munit.CatsEffectSuite
import org.slf4j.LoggerFactory
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

object Parent {

  //  this is here to avoid serialising the entire test class
  val sourceInstance: Source = Source[String](
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
}

class DynamicClassLoaderTest extends CatsEffectSuite {
  val logger = Logger(LoggerFactory.getLogger(this.getClass))

  val minimalTestCode: String =
    """
      |import com.raphtory.api.analysis.algorithm.Generic
      |import com.raphtory.api.analysis.graphview.GraphPerspective
      |import com.raphtory.internals.communication.SchemaProviderInstances._
      |
      |object MinimalTestAlgorithm extends Generic {
      |  case class ArbitraryMessage()
      |  override def apply(graph: GraphPerspective): graph.Graph =
      |    graph.step{ vertex =>
      |    import vertex._
      |     vertex.messageAllNeighbours(ArbitraryMessage())
      |    }
      |}
      |MinimalTestAlgorithm
      |""".stripMargin

  private val tb: ToolBox[universe.type] = runtimeMirror(getClass.getClassLoader).mkToolBox()

  private val compiledAlgo: Generic = tb.compile(tb.parse(minimalTestCode))().asInstanceOf[Generic]

  lazy val remoteGraphResource: Resource[IO, DeployedTemporalGraph] =
    Resource.make(IO(remote().newGraph()))(graph => IO(graph.destroy()))

  lazy val remoteProcess =
    Resource.make {
      for {
        started   <- Semaphore[IO](0)
//         get classpath from sbt
        _         <- IO(logger.info("retrieving class path from sbt"))
        classPath <- IO.blocking(Seq("sbt", "--error", "export core/test:fullClasspath").!!)
        process   <- IO {
                       logger.info("starting remote process")
                       Process(Seq("java", "-cp", classPath, "com.raphtory.service.Standalone")).run(
                               ProcessLogger { line =>
                                 if (line == "Raphtory service started")
                                   started.release.unsafeRunSync()
                                 println(line) // forward output
                               }
                       )
                     }
//         start tests if there is already a standalone instance
        _         <- (IO.blocking(process.exitValue()) *> started.release *> IO(
                               logger.info("Failed to start remote process, a standalone instance is likely already running")
                       )).start
//        head node is started, proceed
        _         <- started.acquire
      } yield process
    }(process =>
      IO {
        logger.info("destroying remote process")
        process.destroy()
      }
    )

  lazy val remoteGraph = ResourceFixture(
          remoteGraphResource
            .evalMap(g => IO(g.addDynamicPath("com.raphtory.lotrtest")))
            .evalMap(g => IO(g.load(source())))
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
            source <- Resource.pure(CSVEdgeListSource(FileSpout("/tmp/lotr.csv")))
          } yield source
  )

  lazy val sourceInline: Fixture[Source] = ResourceSuiteLocalFixture[Source](
          "lotr-inline",
          for {
            _      <- TestUtils.manageTestFile(
                              Some("/tmp/lotr.csv", new URL("https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"))
                      )
            source <- Resource.pure(
                              Parent.sourceInstance
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
          "remote",
          for {
            _          <- remoteProcess
            connection <- Resource.make {
                            IO {
                              logger.info("connecting to remote")
                              Raphtory.connect()
                            }
                          } { c =>
                            IO {
                              logger.info("closing remote connection")
                              // c.close() // TODO: this currently falls over if any graphs were closed before
                            }
                          }
          } yield connection
  )

  override def munitFixtures = List(remote, source, localGraph, sourceInline)

  test("test algorithm locally") {
    val res = localGraph().execute(MaxFlowTest[Int]("Gandalf", "Gandalf")).get().toList
    assert(res.nonEmpty)
  }

  test("test locally compiled algo") {
    val res = localGraph().execute(compiledAlgo).get().toList
    assert(res.nonEmpty)
  }

  remoteGraph.test("When an algo crashes and we try to iterate over the result we get an exception") { g =>
    val res = Try(g.select(vertex => Row(1)).get().toList)
    assert(res.isFailure)
  }

  remoteGraph.test("When an algo crashes waitForJob returns and isJobDone is false") { g =>
    val query = g.select(vertex => Row(1)).filter(row => false).writeTo(PrintSink())
    intercept[java.lang.RuntimeException](query.waitForJob())
    assert(!query.isJobDone)
  }

  remoteGraph.test("test algorithm class injection") { g =>
    val res = g.execute(MinimalTestAlgorithm).get().toList
    assert(res.nonEmpty)
  }

  remoteGraph.test("test manual dynamic path") { g =>
    val res = g.addDynamicPath("dependency").execute(TestAlgorithmWithExternalDependency).get().toList
    assert(res.nonEmpty)
  }

  remoteGraphInline.test("test inline graphbuilder definition") { g =>
    val res = g.execute(EdgeList()).get().toList
    assert(res.nonEmpty)
  }

  remoteGraph.test("test algorithm class injection with MaxFlow") { g =>
    val res = g.execute(MaxFlowTest[Int]("Gandalf", "Gandalf")).get().toList
    assert(res.nonEmpty)
  }
}
