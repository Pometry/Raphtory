package com.raphtory.core.components.akkamanagement

import akka.actor.ExtendedActorSystem
import com.esotericsoftware.kryo.serializers.ClosureSerializer.Closure
import com.twitter.chill.{AllScalaRegistrar, KryoBase, KryoInstantiator, ScalaKryoInstantiator}
import com.twitter.chill.akka.{ActorRefSerializer, AkkaSerializer}
import com.esotericsoftware.kryo.serializers.ClosureSerializer

import java.lang.invoke.SerializedLambda

class RaphtoryKryoInstantiator extends ScalaKryoInstantiator {
  override def newKryo: KryoBase = {
    val k = super.newKryo
    k.setRegistrationRequired(false)
    k.register(classOf[SerializedLambda])
    k.register(classOf[Closure], new ClosureSerializer)
    val reg = new AllScalaRegistrar
    reg(k)
    k
  }
}

class RaphtoryAkkaSerialiser(system: ExtendedActorSystem) extends AkkaSerializer(system){
  override def kryoInstantiator: KryoInstantiator = {
    (new RaphtoryKryoInstantiator).withRegistrar(new ActorRefSerializer(system))
  }
}
