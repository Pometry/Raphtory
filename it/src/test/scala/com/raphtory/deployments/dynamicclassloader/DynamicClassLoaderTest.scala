package com.raphtory.deployments.dynamicclassloader

import cats.effect.IO
import cats.effect.kernel.Resource
import com.raphtory.TestUtils
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.api.analysis.algorithm._
import com.raphtory.api.analysis.graphview.TemporalGraph
import com.raphtory.api.analysis.table.{KeyPair, Row}
import com.raphtory.api.input._
import com.raphtory.internals.context.{RaphtoryContext, RaphtoryIOContext}
import com.raphtory.internals.management.GraphConfig.ConfigBuilder
import com.raphtory.sinks.PrintSink
import com.raphtory.sources.CSVEdgeListSource
import com.raphtory.spouts.FileSpout
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import grpc.health.v1.health.{Health, HealthCheckRequest}
import higherkindness.mu.rpc.ChannelForAddress
import munit.CatsEffectSuite
import org.slf4j.LoggerFactory

import java.net.URL
import scala.concurrent.duration.DurationInt
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
                    Properties(ImmutableString("name", sourceNode)),
                    Type("Character")
            )
            graph.addVertex(
                    timeStamp,
                    tarID,
                    Properties(ImmutableString("name", targetNode)),
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

  private val defaultConf: Config = ConfigBuilder.getDefaultConfig

  private val waitForRaphtoryService: IO[Unit] =
    Health.client[IO](ChannelForAddress("localhost", defaultConf.getInt("raphtory.deploy.port"))).use { client =>
      for {
        serving <- client.Check(HealthCheckRequest()).map(response => response.status.isServing).handleError(_ => false)
        _       <- if (serving) IO.unit else IO.sleep(1.seconds) *> waitForRaphtoryService
      } yield ()
    }

  lazy val remoteProcess =
    Resource.make {
      for {
//         get classpath from sbt
        _         <- IO(logger.info("retrieving class path from sbt"))
        classPath <- IO.blocking(Seq("sbt", "--error", "export core/test:fullClasspath").!!)
        _         <- IO(logger.info("starting remote process"))
        process   <- IO(Process(Seq("java", "-cp", classPath, "com.raphtory.service.Standalone")).run())
//         start tests if there is already a standalone instance
        _         <- (IO.blocking(process.exitValue()) *> IO(
                               logger.info("Failed to start remote process, a standalone instance is likely already running")
                       )).start
        _         <- waitForRaphtoryService
      } yield process
    }(process =>
      IO {
        logger.info("destroying remote process")
        process.destroy()
      }
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

  sealed trait Context

  case object LocalContext extends Context

  case object RemoteContext extends Context

  lazy val localContextFixture: Fixture[RaphtoryContext] = ResourceSuiteLocalFixture(
          "local",
          RaphtoryIOContext.localIO()
  )

  lazy val remoteContextFixture: Fixture[RaphtoryContext] = ResourceSuiteLocalFixture(
          "remote",
          for {
            _       <- remoteProcess
            context <- RaphtoryIOContext.remoteIO()
          } yield context
  )

  def fetchContext(context: Context): Fixture[RaphtoryContext] =
    context match {
      case LocalContext  => localContextFixture
      case RemoteContext => remoteContextFixture
    }

  override def munitFixtures = List(localContextFixture, remoteContextFixture, source, sourceInline)

  test("test algorithm locally") {
    val res = runWithGraph(LocalContext, source) { graph =>
      graph.addDynamicPath("com.raphtory.deployments.dynamicclassloader").execute(MaxFlowTest[Int]("Gandalf", "Gandalf")).get().toList
    }
    assert(res.nonEmpty)
  }

  test("test locally compiled algo") {
    val res = runWithGraph(LocalContext, source) { graph =>
      graph.execute(compiledAlgo).get().toList
    }
    assert(res.nonEmpty)
  }

  test("When an algo crashes and we try to iterate over the result we get an exception") {
    val res = runWithGraph(RemoteContext, source) { graph =>
      Try(graph.select(vertex => Row(KeyPair("", 1))).get().toList)
    }
    assert(res.isFailure)
  }

  test("When an algo crashes waitForJob returns and isJobDone is false") {
    val res = runWithGraph(RemoteContext, source) { graph =>
      val query = graph.select(vertex => Row(KeyPair("",1))).filter(row => false).writeTo(PrintSink())
      intercept[java.lang.RuntimeException](query.waitForJob())
      !query.isJobDone
    }
    assert(res)
  }

  test("test algorithm class injection") {
    val res = runWithGraph(RemoteContext, source) { graph =>
      graph.addDynamicPath("com.raphtory.deployments.dynamicclassloader").execute(MinimalTestAlgorithm).get().toList
    }
    assert(res.nonEmpty)
  }

  test("test manual dynamic path") {
    val res = runWithGraph(RemoteContext, source) { graph =>
      graph.addDynamicPath("com.raphtory.deployments.dynamicclassloader").execute(TestAlgorithmWithExternalDependency).get().toList
    }
    assert(res.nonEmpty)
  }

  test("test inline graphbuilder definition") {
    val res = runWithGraph(RemoteContext, sourceInline) { graph =>
      graph.execute(EdgeList()).get().toList
    }
    assert(res.nonEmpty)
  }

  test("test algorithm class injection with MaxFlow") {
    val res = runWithGraph(RemoteContext, source) { graph =>
      graph.addDynamicPath("com.raphtory.deployments.dynamicclassloader").execute(MaxFlowTest[Int]("Gandalf", "Gandalf")).get().toList
    }
    assert(res.nonEmpty)
  }

  private def runWithGraph[T](context: Context, source: Fixture[Source])(graphHandling: TemporalGraph => T): T =
    fetchContext(context)().runWithNewGraph() { graph =>
      val graphWithLoad = graph.addDynamicPath("com.raphtory.deployments.dynamicclassloader").load(source())
      graphHandling(graphWithLoad)
    }
}
