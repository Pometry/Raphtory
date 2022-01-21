package com.raphtory.algorithms

import com.raphtory.RaphtoryPD
import org.scalatest.FunSuite
import akka.pattern.ask
import akka.util.Timeout
import com.raphtory.spouts.FileSpout
import com.raphtory.core.components.spout.Spout
import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.model.graph.{ImmutableProperty, Properties}
import com.raphtory.algorithms.generic.ThreeNodeMotifs
import com.raphtory.core.build.server.RaphtoryGraph
import com.raphtory.core.components.querymanager.QueryManager.Message.{AreYouFinished, ManagingTask, PointQuery, RangeQuery, TaskFinished, Windows}
import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row, Table}

import java.io.File
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

trait Message {
  def data: String
}

case class Node(data: String) extends Message
case class Edge(data: String) extends Message

class VertexEdgeSpout(vertexSpout: Spout[String], edgeSpout: Spout[String]) extends Spout[Message] {
  override def setupDataSource(): Unit = {
    vertexSpout.setupDataSource()
    edgeSpout.setupDataSource()
  }

  def generateNode(): Option[Node] = {
    if (! vertexSpout.isComplete()) {
      vertexSpout.generateData() match {
        case Some(value) => Some(Node(value))
        case None => None
      }
    } else {
      None
    }
  }

  def generateEdge(): Option[Edge] = {
    if (! edgeSpout.isComplete()) {
      edgeSpout.generateData() match {
        case Some(value) => Some(Edge(value))
        case None => None
      }
    } else {
      None
    }
  }

  override def generateData(): Option[Message] = {
    generateNode() match {
      case Some(value) => Some(value)
      case None => generateEdge() match {
        case Some(value) => Some(value)
        case None =>
          dataSourceComplete()
          None
      }
    }
  }


  override def closeDataSource(): Unit = {
    vertexSpout.closeDataSource()
    edgeSpout.closeDataSource()
  }
}

object VertexEdgeSpout {
  def apply(vertexSpout: Spout[String], edgeSpout: Spout[String]) = new VertexEdgeSpout(vertexSpout: Spout[String], edgeSpout: Spout[String])
}


class ResourceSpout(resourceName: String, dropHeader: Boolean = false) extends Spout[String] {
  val fileQueue = mutable.Queue[String]()

  override def setupDataSource(): Unit = {
    try {
      if (dropHeader) {
        fileQueue ++= scala.io.Source.fromResource(resourceName)
          .getLines.drop(1)
      } else {
        fileQueue ++= scala.io.Source.fromResource(resourceName)
          .getLines
      }
    } catch {
      case _: Throwable => dataSourceComplete()
    }
  }//no setup

  override def generateData(): Option[String] = {
    if(fileQueue.isEmpty){
      dataSourceComplete()
      None
    }
    else
      Some(fileQueue.dequeue())
  }
  override def closeDataSource(): Unit = {}//no file closure already done
}

object ResourceSpout {
  def apply(resourceName: String, dropHeader: Boolean = false) = new ResourceSpout(resourceName, dropHeader)
}


class MotifTestGraphBuilder extends GraphBuilder[Message]{
  override def parseTuple(tuple: Message) = tuple match {
    case Edge(data) => {
      val fileLine = data.split(",").map(_.trim)
      val sourceNode = fileLine(0)
      val srcID = assignID(sourceNode)
      val targetNode = fileLine(1)
      val tarID = assignID(targetNode)
      val timeStamp = fileLine(2).toLong

      addVertex(timeStamp, srcID, Properties(ImmutableProperty("name", sourceNode)))
      addVertex(timeStamp, tarID, Properties(ImmutableProperty("name", targetNode)))
      addEdge(timeStamp, srcID, tarID)
    }
    case Node(data) => {
      val fileLine = data.split(",").map(_.trim)
      val node = fileLine(0)
      val id = assignID(node)
      val correct_values = fileLine.drop(1).mkString(",")
      addVertex(0, id, Properties(ImmutableProperty("name", node), ImmutableProperty("correctCounts", correct_values)))
    }
  }
}

object MotifTestGraphBuilder {
  def apply() = new MotifTestGraphBuilder()
}

class CheckResults extends GraphAlgorithm {
  override def apply(graph: GraphPerspective): GraphPerspective = {
    graph.step(vertex => {
      val correctCounts = vertex.getPropertyOrElse[String]("correctCounts", "").split(',').map(_.toLong)
      val recoveredCounts = vertex.getState[ArrayBuffer[Long]]("motifCounts")
      vertex.setState("correct", correctCounts.sameElements(recoveredCounts))
    })
  }

  override def tabularise(graph: GraphPerspective): Table = {
    graph.select(vertex => Row(vertex.getPropertyOrElse("name", "unknown"), vertex.getState[Boolean]("correct"))).filter(row => row(1) == true)
  }

  override def write(table: Table) = {
    table.writeTo("/tmp/ThreeNodeMotifsTest")
  }
}

object CheckResults {
  def apply() = new CheckResults()
}




class ThreeNodeMotifsTest extends FunSuite{
  val testDir = "/tmp/ThreeNodeMotifsTest" //TODO CHANGE TO USER PARAM
//  val spout = VertexEdgeSpout(vertexSpout = new FileSpout(".", "motiftestCorrectResults.csv", dropHeader = true), edgeSpout = new FileSpout(".", "motiftest.csv"))
  val spout = VertexEdgeSpout(vertexSpout = ResourceSpout("motiftestCorrectResults.csv", dropHeader = true), edgeSpout = ResourceSpout("motiftest.csv"))
  val node = RaphtoryPD(spout = spout, graphBuilder = MotifTestGraphBuilder(), port=2000)
  val queryManager    = node.getQueryManager()
  val queryName = "test"

  test("test motif counting") {
    try {
      val result = testMotifs()
      assert(result)
    }
    catch {
      case _: java.util.concurrent.TimeoutException =>
        assert(false)
    }
  }

  def testMotifs(time: Int=100): Boolean = {
    implicit val timeout: Timeout = time.second
    val startingTime = System.currentTimeMillis()
    val resultDir = new File(testDir, queryName)
    if (! resultDir.exists()) {
      resultDir.mkdir()
    } else {
      resultDir.listFiles().foreach(f => f.delete())
    }
    queryManager ! PointQuery(queryName,ThreeNodeMotifs() -> CheckResults(), 23)
    //val future = queryManager ? PointQuery(queryName,funcs, 299860)
    Thread.sleep(100000)
    println(s"Task completed in ${System.currentTimeMillis()-startingTime} milliseconds")
    val dir = new File(testDir+s"/$queryName").listFiles.filter(_.isFile)
    val lines = (for(i <- dir) yield scala.io.Source.fromFile(i).getLines().toList).flatten
    lines.length == 11
  }

}
