package com.raphtory.core.components.akkamanagement


import com.raphtory.core.components.leader.WatchDog.Message._
import com.raphtory.core.components.leader.WatermarkManager.Message._
import com.raphtory.core.components.spout.SpoutAgent.Message._
import com.raphtory.core.components.graphbuilder.BuilderExecutor.Message._
import com.raphtory.core.model.graph._
import com.raphtory.core.components.querymanager.QueryHandler.Message._
import com.raphtory.core.components.querymanager.QueryManager.Message._
import io.altoo.akka.serialization.kryo.DefaultKryoInitializer
import io.altoo.akka.serialization.kryo.serializer.scala.ScalaKryo
import com.esotericsoftware.kryo.serializers.ClosureSerializer.Closure
import com.raphtory.algorithms.PartitionState
import com.raphtory.core.model.algorithm._
import com.raphtory.dev.wordSemantic.spouts.Update
import akka.actor.{ExtendedActorSystem, RepointableActorRef}
import akka.remote.RemoteActorRef
import com.esotericsoftware.kryo.serializers.ClosureSerializer

import java.lang.invoke.SerializedLambda
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.immutable.ParHashMap
import scala.collection.parallel.mutable.ParArray


class RaphtoryKryoInitializer extends DefaultKryoInitializer{
  println("hello")
  override def initScalaSerializer(kryo: ScalaKryo, system: ExtendedActorSystem): Unit = {
    super.initScalaSerializer(kryo, system)
    println("hello test")
    registerLeaderMessages(kryo)
    registerSpoutMessages(kryo)
    registerBuilderMessages(kryo)
    registerPartitionMessages(kryo)
    registerMiscMessages(kryo)
    registerJavaScala(kryo)
  }

  override def preInit(kryo: ScalaKryo): Unit = {
    super.preInit(kryo)
    registerLeaderMessages(kryo)
    registerSpoutMessages(kryo)
    registerBuilderMessages(kryo)
    registerPartitionMessages(kryo)
    registerMiscMessages(kryo)
    registerJavaScala(kryo)
  }

  override def initAkkaSerializer(kryo: ScalaKryo, system: ExtendedActorSystem): Unit = {
    super.initAkkaSerializer(kryo, system)
    registerLeaderMessages(kryo)
    registerSpoutMessages(kryo)
    registerBuilderMessages(kryo)
    registerPartitionMessages(kryo)
    registerMiscMessages(kryo)
    registerJavaScala(kryo)
  }

  override def postInit(kryo: ScalaKryo): Unit = {
    super.postInit(kryo)
    registerLeaderMessages(kryo)
    registerSpoutMessages(kryo)
    registerBuilderMessages(kryo)
    registerPartitionMessages(kryo)
    registerMiscMessages(kryo)
    registerJavaScala(kryo)


    kryo.register(classOf[SerializedLambda])
    kryo.register(classOf[Closure], new ClosureSerializer)
  }

  private def registerLeaderMessages(kryo:ScalaKryo) = {
    //WATCHDOG
    kryo.register(RequestBuilderId.getClass)
    kryo.register(ClusterStatusRequest.getClass)
    kryo.register(RequestPartitionId.getClass)
    kryo.register(RequestSpoutId.getClass)
    kryo.register(RequestQueryId.getClass)
    kryo.register(PartitionsCount.getClass)
    kryo.register(ClusterStatusResponse.getClass)
    kryo.register(AssignedId.getClass)
    kryo.register(BuilderUp.getClass)
    kryo.register(SpoutUp.getClass)
    kryo.register(QueryManagerUp.getClass)
    kryo.register(PartitionUp.getClass)
    kryo.register(RequestPartitionCount.getClass)
    //WATERMARKER
    kryo.register(ProbeWatermark.getClass)
    kryo.register(WatermarkTime.getClass)
    kryo.register(SaveState.getClass)
    kryo.register(WhatsTheTime.getClass)
  }

  private def registerSpoutMessages(kryo:ScalaKryo) = {
    kryo.register(SpoutOnline.getClass)
    kryo.register(WorkPlease.getClass)
    kryo.register(AllocateTuple.getClass)
    kryo.register(DataFinished.getClass)
    kryo.register(NoWork.getClass)
  }

  private def registerBuilderMessages(kryo:ScalaKryo) = {
    kryo.register(BuilderTimeSync.getClass)
    kryo.register(DataFinishedSync.getClass)
    kryo.register(KeepAlive.getClass)

  }

  private def registerPartitionMessages(kryo:ScalaKryo) = {
    kryo.register(TrackedGraphUpdate.getClass)
    kryo.register(VertexAdd.getClass)
    kryo.register(EdgeAdd.getClass)
    kryo.register(VertexDelete.getClass)
    kryo.register(EdgeDelete.getClass)
    kryo.register(Properties.getClass)
    kryo.register(DoubleProperty.getClass)
    kryo.register(ImmutableProperty.getClass)
    kryo.register(StringProperty.getClass)
    kryo.register(LongProperty.getClass)
    kryo.register(DoubleProperty.getClass)
    kryo.register(Type.getClass)

    kryo.register(TrackedGraphEffect.getClass)
    kryo.register(SyncNewEdgeAdd.getClass)
    kryo.register(SyncExistingEdgeAdd.getClass)
    kryo.register(SyncExistingEdgeRemoval.getClass)
    kryo.register(SyncNewEdgeRemoval.getClass)
    kryo.register(OutboundEdgeRemovalViaVertex.getClass)
    kryo.register(InboundEdgeRemovalViaVertex.getClass)
    kryo.register(SyncExistingRemovals.getClass)
    kryo.register(EdgeSyncAck.getClass)
    kryo.register(VertexRemoveSyncAck.getClass)
    kryo.register(VertexMessage.getClass)

  }
  private def registerQueryMessages(kryo:ScalaKryo) = {
    //QUERY MANAGER
    kryo.register(KillTask.getClass)
    kryo.register(JobKilled.getClass)
    kryo.register(ResultsForApiPI.getClass)
    kryo.register(JobDoesntExist.getClass)
    kryo.register(ManagingTask.getClass)
    kryo.register(AreYouFinished.getClass)
    kryo.register(TaskFinished.getClass)
    kryo.register(JobFailed.getClass)

    kryo.register(PointQuery.getClass)
    kryo.register(RangeQuery.getClass)
    kryo.register(LiveQuery.getClass)
    kryo.register(QueryNotPresent.getClass)

    //ALGORITHM
    kryo.register(Step.getClass)
    kryo.register(Iterate.getClass)
    kryo.register(VertexFilter.getClass)
    kryo.register(Select.getClass)
    kryo.register(TableFilter.getClass)
    kryo.register(WriteTo.getClass)



    //QUERY HANDLER
    kryo.register(StartAnalysis.getClass)
    kryo.register(ReaderWorkersOnline.getClass)
    kryo.register(ReaderWorkersAck.getClass)
    kryo.register(LoadAnalyser.getClass)
    kryo.register(ExecutorEstablished.getClass)
    kryo.register(TimeCheck.getClass)
    kryo.register(TimeResponse.getClass)
    kryo.register(RecheckTime.getClass)
    kryo.register(CreatePerspective.getClass)
    kryo.register(PerspectiveEstablished.getClass)
    kryo.register(StartSubtask.getClass)
    kryo.register(Ready.getClass)
    kryo.register(SetupNextStep.getClass)
    kryo.register(SetupNextStepDone.getClass)
    kryo.register(StartNextStep.getClass)
    kryo.register(CheckMessages.getClass)
    kryo.register(GraphFunctionComplete.getClass)
    kryo.register(EndStep.getClass)
    kryo.register(Finish.getClass)
    kryo.register(ReturnResults.getClass)
    kryo.register(StartNextSubtask.getClass)
    kryo.register(EstablishExecutor.getClass)
    kryo.register(ExecutorEstablished.getClass)
    kryo.register(StartGraph.getClass)
    kryo.register(TableBuilt.getClass)
    kryo.register(TableFunctionComplete.getClass)
    kryo.register(CreatePerspective.getClass)
    kryo.register(PerspectiveEstablished.getClass)



  }
  private def registerMiscMessages(kryo:ScalaKryo) = {
    kryo.register(Update.getClass)
    kryo.register(PartitionState.getClass)

  }

  private def registerJavaScala(kryo:ScalaKryo) = {
    kryo.register(mutable.TreeMap.getClass)
    kryo.register(None.getClass)
    kryo.register(Tuple2.getClass)
    kryo.register(Tuple3.getClass)
    kryo.register(Tuple4.getClass)
    kryo.register(Tuple5.getClass)
    kryo.register(Tuple6.getClass)
    kryo.register(Tuple7.getClass)
    kryo.register(Tuple8.getClass)
    kryo.register(Tuple9.getClass)
    kryo.register(Tuple10.getClass)
    kryo.register(Tuple11.getClass)
    kryo.register(Tuple12.getClass)
    kryo.register(Tuple13.getClass)
    kryo.register(Tuple14.getClass)
    kryo.register(Tuple15.getClass)
    kryo.register(Tuple16.getClass)
    kryo.register(Tuple17.getClass)
    kryo.register(Tuple18.getClass)
    kryo.register(Tuple19.getClass)
    kryo.register(Tuple20.getClass)
    kryo.register(Tuple21.getClass)
    kryo.register(Tuple22.getClass)
    kryo.register(ArrayBuffer.getClass)
    kryo.register(Array.getClass)
    kryo.register(mutable.HashMap.getClass)
    kryo.register(ParHashMap.getClass)
    kryo.register(ParHashMap.getClass)
    kryo.register(ParArray.getClass)
    kryo.register(mutable.ArraySeq.getClass)
    kryo.register(mutable.WrappedArray.getClass)


  }

}
