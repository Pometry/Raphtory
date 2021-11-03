package com.raphtory.core.serializer

import akka.actor.ExtendedActorSystem
import akka.serialization.Serializer
import com.twitter.chill._
import com.twitter.chill.akka.{ActorRefSerializer, AkkaConfig}
import com.twitter.chill.config.ConfiguredInstantiator

class AkkaSerializer(system: ExtendedActorSystem) extends Serializer {

  /**
    * You can override this to easily change your serializers. If you do so, make sure to change the config to
    * use the name of your new class
    */
  def kryoInstantiator: KryoInstantiator = {
    val instantiator = new EmptyScalaKryoInstantiator
    val kryo = instantiator.newKryo()

    (new ScalaKryoInstantiator).withRegistrar(new ActorRefSerializer(system))
  }

  /**
    * Since each thread only needs 1 Kryo, the pool doesn't need more space than the number of threads. We
    * guess that there are 4 hyperthreads / core and then multiple by the nember of cores.
    */
  def poolSize: Int = {
    val GUESS_THREADS_PER_CORE = 4
    GUESS_THREADS_PER_CORE * Runtime.getRuntime.availableProcessors
  }

  val kryoPool: KryoPool =
    KryoPool.withByteArrayOutputStream(poolSize, kryoInstantiator)

  def includeManifest: Boolean = false
  def identifier = 8675309
  def toBinary(obj: AnyRef): Array[Byte] = kryoPool.toBytesWithClass(obj)
  def fromBinary(bytes: Array[Byte], clazz: Option[Class[_]]): AnyRef =
    kryoPool.fromBytes(bytes)
}

/**
  * Uses the Config system of chill.config to Configure at runtime which KryoInstantiator to use Overriding
  * kryoInstantiator and using your own class name is probably easier for most cases. See
  * ConfiguredInstantiator static methods for how to build up a correct Config with your reflected or
  * serialized instantiators.
  */
class ConfiguredAkkaSerializer(system: ExtendedActorSystem) extends AkkaSerializer(system) {
  override def kryoInstantiator: KryoInstantiator =
    new ConfiguredInstantiator(new AkkaConfig(system.settings.config))
      .withRegistrar(new ActorRefSerializer(system))
}