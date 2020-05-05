
import com.raphtory.core.analysis.API.{Analyser, BlankAnalyser}
import scala.language.postfixOps
import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox

//val newText =  "import scala.collection.mutable.ArrayBuffer\r\nimport com.raphtory.core.analysis.API.Analyser\r\nclass Blank(args:Array[String]) extends Analyser(args) {\r\n  override def analyse(): Unit = {}\r\n  override def setup(): Unit = {}\r\n  override def returnResults(): Any = {}\r\n  override def defineMaxSteps(): Int = 1\r\n  override def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = {println(\"howdy!\")}\r\n}"

object Files {
  case class Person(a: String, b: Int)

  val analyser = """
    | import scala.collection.mutable.ArrayBuffer
    |  import com.raphtory.core.analysis.API.Analyser
    |  class Blanklyser(args:Array[String]) extends Analyser(args) {
    |    val myUniqString = 1
    |    override def analyse(): Unit = {println(args(0))}
    |    override def setup(): Unit = {}
    |    override def returnResults(): Any = {}
    |    override def defineMaxSteps(): Int = 1
    |    override def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = {println("howdy!")}
    | }
    | scala.reflect.classTag[Blanklyser].runtimeClass
    |""".stripMargin

  val test =
    """
      | class Person(a: String, b: Int)
      |""".stripMargin
}

case class LoadExternalAnalyser(rawFile: String,args:Array[String]) {
  val toolbox = currentMirror.mkToolBox()
  val tree = toolbox.parse(rawFile)
  val compiledCode = toolbox.compile(tree).apply().asInstanceOf[Class[Analyser]]
  def newAnalyser = compiledCode.getConstructor(classOf[Array[String]]).newInstance(args).asInstanceOf[Analyser]

}

 try{
    val analyser = LoadExternalAnalyser(Files.analyser,Array("null")).newAnalyser.analyse()
    println(analyser)
  }





