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
  var id = 1000
  def nextID() = {
    id+=1
    id
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
    kryo.register(RequestBuilderId.getClass,nextID())
    kryo.register(ClusterStatusRequest.getClass,nextID())
    kryo.register(RequestPartitionId.getClass,nextID())
    kryo.register(RequestSpoutId.getClass,nextID())
    kryo.register(RequestQueryId.getClass,nextID())
    kryo.register(PartitionsCount.getClass,nextID())
    kryo.register(ClusterStatusResponse.getClass,nextID())
    kryo.register(AssignedId.getClass,nextID())
    kryo.register(BuilderUp.getClass,nextID())
    kryo.register(SpoutUp.getClass,nextID())
    kryo.register(QueryManagerUp.getClass,nextID())
    kryo.register(PartitionUp.getClass,nextID())
    kryo.register(RequestPartitionCount.getClass,nextID())
    //WATERMARKER
    kryo.register(ProbeWatermark.getClass,nextID())
    kryo.register(WatermarkTime.getClass,nextID())
    kryo.register(SaveState.getClass,nextID())
    kryo.register(WhatsTheTime.getClass,nextID())
  }

  private def registerSpoutMessages(kryo:ScalaKryo) = {
    kryo.register(SpoutOnline.getClass,nextID())
    kryo.register(WorkPlease.getClass,nextID())
    kryo.register(AllocateTuple.getClass,nextID())
    kryo.register(DataFinished.getClass,nextID())
    kryo.register(NoWork.getClass,nextID())
  }

  private def registerBuilderMessages(kryo:ScalaKryo) = {
    kryo.register(BuilderTimeSync.getClass,nextID())
    kryo.register(DataFinishedSync.getClass,nextID())
    kryo.register(KeepAlive.getClass,nextID())

  }

  private def registerPartitionMessages(kryo:ScalaKryo) = {
    kryo.register(VertexAdd.getClass,nextID())
    kryo.register(EdgeAdd.getClass,nextID())
    kryo.register(VertexDelete.getClass,nextID())
    kryo.register(EdgeDelete.getClass,nextID())
    kryo.register(Properties.getClass,nextID())
    kryo.register(DoubleProperty.getClass,nextID())
    kryo.register(FloatProperty.getClass, nextID())
    kryo.register(ImmutableProperty.getClass,nextID())
    kryo.register(StringProperty.getClass,nextID())
    kryo.register(LongProperty.getClass,nextID())
    kryo.register(DoubleProperty.getClass,nextID())
    kryo.register(Type.getClass,nextID())
    kryo.register(TrackedGraphUpdate.getClass,nextID())

    kryo.register(TrackedGraphEffect.getClass,nextID())
    kryo.register(SyncNewEdgeAdd.getClass,nextID())
    kryo.register(SyncExistingEdgeAdd.getClass,nextID())
    kryo.register(SyncExistingEdgeRemoval.getClass,nextID())
    kryo.register(SyncNewEdgeRemoval.getClass,nextID())
    kryo.register(OutboundEdgeRemovalViaVertex.getClass,nextID())
    kryo.register(InboundEdgeRemovalViaVertex.getClass,nextID())
    kryo.register(SyncExistingRemovals.getClass,nextID())
    kryo.register(EdgeSyncAck.getClass,nextID())
    kryo.register(VertexRemoveSyncAck.getClass,nextID())
    kryo.register(VertexMessage.getClass,nextID())

  }
  private def registerQueryMessages(kryo:ScalaKryo) = {
    //QUERY MANAGER
    kryo.register(KillTask.getClass,nextID())
    kryo.register(JobKilled.getClass,nextID())
    kryo.register(ResultsForApiPI.getClass,nextID())
    kryo.register(JobDoesntExist.getClass,nextID())
    kryo.register(ManagingTask.getClass,nextID())
    kryo.register(AreYouFinished.getClass,nextID())
    kryo.register(TaskFinished.getClass,nextID())
    kryo.register(JobFailed.getClass,nextID())

    kryo.register(PointQuery.getClass,nextID())
    kryo.register(RangeQuery.getClass,nextID())
    kryo.register(LiveQuery.getClass,nextID())
    kryo.register(QueryNotPresent.getClass,nextID())

    //ALGORITHM
    kryo.register(Step.getClass,nextID())
    kryo.register(Iterate.getClass,nextID())
    kryo.register(VertexFilter.getClass,nextID())
    kryo.register(Select.getClass,nextID())
    kryo.register(TableFilter.getClass,nextID())
    kryo.register(WriteTo.getClass,nextID())



    //QUERY HANDLER
    kryo.register(StartAnalysis.getClass,nextID())
    kryo.register(ReaderWorkersOnline.getClass,nextID())
    kryo.register(ReaderWorkersAck.getClass,nextID())
    kryo.register(LoadAnalyser.getClass,nextID())
    kryo.register(ExecutorEstablished.getClass,nextID())
    kryo.register(TimeCheck.getClass,nextID())
    kryo.register(TimeResponse.getClass,nextID())
    kryo.register(RecheckTime.getClass,nextID())
    kryo.register(CreatePerspective.getClass,nextID())
    kryo.register(PerspectiveEstablished.getClass,nextID())
    kryo.register(StartSubtask.getClass,nextID())
    kryo.register(Ready.getClass,nextID())
    kryo.register(SetupNextStep.getClass,nextID())
    kryo.register(SetupNextStepDone.getClass,nextID())
    kryo.register(StartNextStep.getClass,nextID())
    kryo.register(CheckMessages.getClass,nextID())
    kryo.register(GraphFunctionComplete.getClass,nextID())
    kryo.register(EndStep.getClass,nextID())
    kryo.register(Finish.getClass,nextID())
    kryo.register(ReturnResults.getClass,nextID())
    kryo.register(StartNextSubtask.getClass,nextID())
    kryo.register(EstablishExecutor.getClass,nextID())
    kryo.register(ExecutorEstablished.getClass,nextID())
    kryo.register(StartGraph.getClass,nextID())
    kryo.register(TableBuilt.getClass,nextID())
    kryo.register(TableFunctionComplete.getClass,nextID())
    kryo.register(CreatePerspective.getClass,nextID())
    kryo.register(PerspectiveEstablished.getClass,nextID())



  }
  private def registerMiscMessages(kryo:ScalaKryo) = {
    kryo.register(Update.getClass,nextID())
    kryo.register(PartitionState.getClass,nextID())

  }

  private def registerJavaScala(kryo:ScalaKryo) = {
    kryo.register(Long.getClass,nextID())
    kryo.register(Int.getClass,nextID())
    kryo.register(Nil.getClass,nextID())
    
    kryo.register(Tuple2.getClass,nextID())
    kryo.register(Tuple3.getClass,nextID())
    kryo.register(Tuple4.getClass,nextID())
    kryo.register(Tuple5.getClass,nextID())
    kryo.register(Tuple6.getClass,nextID())
    kryo.register(Tuple7.getClass,nextID())
    kryo.register(Tuple8.getClass,nextID())
    kryo.register(Tuple9.getClass,nextID())
    kryo.register(Tuple10.getClass,nextID())
    kryo.register(Tuple11.getClass,nextID())
    kryo.register(Tuple12.getClass,nextID())
    kryo.register(Tuple13.getClass,nextID())
    kryo.register(Tuple14.getClass,nextID())
    kryo.register(Tuple15.getClass,nextID())
    kryo.register(Tuple16.getClass,nextID())
    kryo.register(Tuple17.getClass,nextID())
    kryo.register(Tuple18.getClass,nextID())
    kryo.register(Tuple19.getClass,nextID())
    kryo.register(Tuple20.getClass,nextID())
    kryo.register(Tuple21.getClass,nextID())
    kryo.register(Tuple22.getClass,nextID())

    kryo.register(Some.getClass,nextID())
    kryo.register(None.getClass,nextID())

    kryo.register(ArrayBuffer.getClass,nextID())
    kryo.register(Array.getClass,nextID())

    kryo.register(ParHashMap.getClass,nextID())
    kryo.register(ParArray.getClass,nextID())

    kryo.register(mutable.TreeMap.getClass,nextID())
    kryo.register(mutable.HashMap.getClass,nextID())
    kryo.register(mutable.ArraySeq.getClass,nextID())
    kryo.register(mutable.WrappedArray.getClass,nextID())
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofRef[_]],nextID())

    kryo.register(Seq.getClass, nextID())
    kryo.register(List.getClass, nextID())
  }

}
