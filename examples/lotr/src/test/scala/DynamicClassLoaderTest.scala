import munit.FunSuite

import scala.sys.process._
import com.raphtory.Raphtory
import com.raphtory.api.analysis.graphview.TemporalGraph
import com.raphtory.api.input.Source
import com.raphtory.examples.lotr.analysis.DegreesSeparation
import com.raphtory.examples.lotr.analysis.FlowAdded
import com.raphtory.examples.lotr.analysis.MaxFlowTest
import com.raphtory.examples.lotr.analysis.NewLabel
import com.raphtory.examples.lotr.analysis.Recheck
import com.raphtory.examples.lotr.analysis.Message
import com.raphtory.examples.lotr.graphbuilders.LOTRGraphBuilder
import com.raphtory.spouts.FileSpout
import com.raphtory.utils.FileUtils

class DynamicClassLoaderTest extends FunSuite {

  val source: Fixture[Source] = new Fixture[Source]("lotr") {

    override def beforeAll(): Unit = {
      val path = "/tmp/lotr.csv"
      val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"
      FileUtils.curlFile(path, url)
    }

    override def apply(): Source = Source(FileSpout("/tmp/lotr.csv"), new LOTRGraphBuilder())
  }

  val standalone = new Fixture[Unit]("standalone") {
    def apply(): Unit = {}
    private var raphtory: Process = _

    override def beforeAll(): Unit = {
      println("testing")
      raphtory = Process(
              Seq(
                      "sbt",
                      "core/runMain com.raphtory.service.Standalone",
                      "testLOTR"
              )
      ).run()
    }

    override def afterAll(): Unit = raphtory.destroy()
  }

  override def munitFixtures = List(standalone, source)

  test("test algorithm locally") {
    val g = Raphtory.newGraph()
    g.load(source())
    g.execute(MaxFlowTest[Int]("Gandalf", "Gandalf")).get().foreach(println)
    println(g.getID)
  }
  test("simple algorithm should work") {
    val c = Raphtory.connect("testLOTR")

    var g: TemporalGraph = c.newGraph()
    g.load(source())
    val tracker          = g.execute(DegreesSeparation()).get()

  }
  test("test algorithm class injection") {
    val c = Raphtory.connect("testLOTR")

    var g: TemporalGraph = c.newGraph()
    g.load(source())
    val algo             = MaxFlowTest[Int]("Gandalf", "Gandalf")
    g = g.addClass(Class.forName("com.raphtory.examples.lotr.analysis.Message"))
    g = g.addClass(classOf[FlowAdded[_, _]])
    g = g.addClass(classOf[Recheck[_]])
    g = g.addClass(classOf[NewLabel[_]])
    val tracker          = g.execute(algo).get()
  }
}
