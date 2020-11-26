import com.raphtory.Raphtory
import com.raphtory.examples.lotr.{LOTRDataSource, LOTRGraphBuilder}

object LOTRDeployment extends App{
  val source  = new LOTRDataSource()
  val builder = new LOTRGraphBuilder()
  Raphtory[String](source,builder)
}
