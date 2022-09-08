import munit.FunSuite

import scala.sys.process._
import com.raphtory.Raphtory
import com.raphtory.api.input.Source
import com.raphtory.examples.lotr.analysis.DegreesSeparation
import com.raphtory.examples.lotr.analysis.MaxFlowTest
import com.raphtory.examples.lotr.graphbuilders.LOTRGraphBuilder
import com.raphtory.spouts.FileSpout
import com.raphtory.utils.FileUtils

class DynamicClassLoaderTest extends FunSuite {

  val standalone = new Fixture[Unit]("standalone") {
    def apply(): Unit = {}
    private var raphtory: Process = _

    override def beforeAll(): Unit = {
      println("testing")
      Process(Seq("sbt", "core/assembly")).!
      raphtory = Process(
              Seq(
                      "java",
                      "-cp",
                      "core/target/scala-2.13/core-assembly-0.1.0.jar",
                      "com.raphtory.service.Standalone",
                      "testLOTR"
              )
      ).run()
    }

    override def afterAll(): Unit = raphtory.destroy()
  }

  override def munitFixtures = List(standalone)
  test("test algorithm class injection") {
    val c      = Raphtory.connect("testLOTR")
    val path   = "/tmp/lotr.csv"
    val url    = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"
    FileUtils.curlFile(path, url)
    val source = Source(FileSpout("/tmp/lotr.csv"), new LOTRGraphBuilder())
    val g      = c.newGraph()
    g.load(source)
    g.execute(MaxFlowTest[Int]("Gandalf", "Gandalf")).get().foreach(println)
    println(g.getID)
  }
}
