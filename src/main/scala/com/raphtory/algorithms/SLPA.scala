package com.raphtory.algorithms

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

import scala.collection.mutable.Queue
import scala.util.Random

/**
Description
  This returns the overlapping communities of the graph by a variant of the synchronous label propagation algorithm. In this algorithm, each node maintains
  a memory of labels. At each step, a vertex chooses a label from its memory to send to all its neighbours according to a Speaking Rule (e.g.
  choose a label at random from memory. Then each vertex chooses a label from its received queue of labels to add to memory according to a Listening Rule
  (e.g. choose the most commonly received label). Instead of converging based on some criterium, the labels a node "remembers" is stored and the
  algorithm terminates after a set number of iterations. At this point, Raphtory returns the name of each vertex and its memory of labels (the size of which is
 the number of iterations).

Parameters
  iterNumber (Int) : Number of iterations to run (default 50).
  speakerRule (Rule) : rule for choosing label to propagate
  listenerRule (Rule) : rule for choosing label to add to mem
  output (String) : Directory path that points to where to store the results. (Default: "/tmp/SLPA")

Returns
  vertex ID (string) : name property or hashed id of each vertex
  memory (array[Long]) : memory of labels (hashed vids)

Notes
  This implementation is based on the paper SLPA: Uncovering Overlapping Communities in Social Networks via A
Speaker-listener Interaction Dynamic Process by Jierui Xie, Boleslaw K. Szymanski and Xiaoming Liu (2011)
  **/

class SLPA(iterNumber: Int = 50,speakerRule:Rule, listenerRule:Rule, output:String= "/tmp/SLPA") extends GraphAlgorithm {

  override def algorithm(graph: GraphPerspective): Unit = {
    graph.step({
      // Initialise vertex memory
      vertex =>
        val memory = Queue(vertex.ID())
        vertex.setState("memory",memory)

        val message = speakerRule.chooseLabel(memory)
        vertex.messageAllNeighbours(message)
    })
      .iterate({
      vertex =>
        val newlab = listenerRule.chooseLabel(Queue(vertex.messageQueue[Long]:_*))
        val memory = vertex.getState[Queue[Long]]("memory")
        memory += newlab

        val message = speakerRule.chooseLabel(memory)
        vertex.messageAllNeighbours(message)
    },executeMessagedOnly = true, iterations = iterNumber)
      .select({
      vertex =>
        val memory = vertex.getState[Queue[Long]]("memory")
        Row(vertex.getPropertyOrElse("name",vertex.ID()), "["+memory.mkString(" ")+"]")
    })
      .writeTo(output)
  }

}

object SLPA {
  def apply(iterNumber: Int = 50, speakerRule:Rule, listenerRule:Rule, output:String= "/tmp/LPA")
  = new SLPA(iterNumber,speakerRule,listenerRule,output)
}

sealed trait Rule {
  def chooseLabel(labels: Queue[Long]) : Long
}

case class Fifo() extends Rule {
  override def chooseLabel(labels: Queue[Long]): Long = {
    labels.dequeue()
  }
}

case class MostCommon() extends Rule {
  override def chooseLabel(labels: Queue[Long]): Long = {
    labels.groupBy(identity).mapValues(_.size).maxBy(_._2)._1
  }
}

case class ChooseRandom(rnd:Random=new Random()) extends Rule {
  override def chooseLabel(labels: Queue[Long]): Long = {
    val asList = labels.toList
    asList(rnd.nextInt(labels.size))
  }
}
