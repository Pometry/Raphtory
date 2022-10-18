import scala.io.Source
import scala.util.Using

object Version {
  val raphtoryVersion      = Using(Source.fromFile("version"))(_.getLines().next()).get
  val raphtoryScalaVersion = "2.13.7"
}
