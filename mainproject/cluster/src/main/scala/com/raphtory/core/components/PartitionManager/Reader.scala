package com.raphtory.core.components.PartitionManager

import akka.actor.{Actor, ActorPath, ActorRef, Props}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.analysis.{Analyser, GraphRepoProxy}
import com.raphtory.core.components.PartitionManager.Workers.{IngestionWorker, ReaderWorker}
import com.raphtory.core.model.communication._
import com.raphtory.core.utils.Utils
import com.twitter.util.Eval

import scala.collection.parallel.mutable.ParTrieMap

class Reader(id : Int, test : Boolean, managerCountVal : Int) extends Actor {
  implicit var managerCount: Int = managerCountVal
  val managerID: Int = id //ID which refers to the partitions position in the graph manager map
  val mediator: ActorRef = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages
  mediator ! DistributedPubSubMediator.Put(self)
  mediator ! DistributedPubSubMediator.Subscribe(Utils.readersTopic, self)
  implicit val proxy = GraphRepoProxy

  val debug = false

  var readers: ParTrieMap[Int,ActorRef] = new ParTrieMap[Int,ActorRef]()
  for(i <- 0 until 10){ //create threads for writing
    val child = context.system.actorOf(Props(new ReaderWorker(managerCount,managerID,i)).withDispatcher("reader-dispatcher"),s"Manager_${id}_reader_$i")
    readers.put(i,child)
  }

  override def preStart() = {
   if(debug)println("Starting reader")
  }

  override def receive: Receive = {
    case ReaderWorkersOnline() => sender() ! ReaderWorkersACK()
    case AnalyserPresentCheck(classname) => presentCheck(classname)
    case CompileNewAnalyser(analyser, name) => compileNewAnalyser(analyser, name)
    case UpdatedCounter(newValue) => managerCount = newValue; readers.foreach(x=> x._2 ! UpdatedCounter(newValue))
    case e => //println(s"[READER] not handled message " + e)
  }

  def presentCheck(classname:String) = {
    try {
      Class.forName(classname)
      if(debug)println(s"Reader has this class can precede: $classname ")
      sender() ! AnalyserPresent()
    }
    catch {
      case e: ClassNotFoundException => {
        if(debug)println("Analyser not found within this image, requesting scala file")
        sender() ! ClassMissing()
      }
    }
  }

  def compileNewAnalyser(analyserString: String, name: String) = {
    if(debug)println(s"Received $name from LAM, compiling")
    try {
      val eval = new Eval // Initializing The Eval without any target location
      val analyser: Analyser = eval[Analyser](analyserString)
      Utils.analyserMap += ((name, analyser))
      sender() ! ClassCompiled()
    }
    catch {
      case e: Exception => {
        sender() ! FailedToCompile(e.getMessage)
      }
    }
  }






}

