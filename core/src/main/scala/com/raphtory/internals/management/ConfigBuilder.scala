package com.raphtory.internals.management

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValue
import com.typesafe.config.ConfigValueFactory
import scala.collection.mutable.ArrayBuffer

private[raphtory] class RuntimeConfig private[management] {
  private lazy val configs  = createConf()
  private val customConfigs = ArrayBuffer[(String, ConfigValue)]()

  private def createConf(): Config = {
    var tempConf =
      ConfigFactory
        .defaultOverrides()
        .withFallback(ConfigFactory.defaultApplication())

    customConfigs.foreach {
      case (path, value) =>
        tempConf = tempConf
          .withValue(
                  path,
                  ConfigValueFactory.fromAnyRef(value)
          )
    }

    tempConf
  }

  private[management] def upsertConfig(path: String, value: Any): Unit =
    customConfigs += ((path, ConfigValueFactory.fromAnyRef(value)))

  def getConfig: Config = configs.resolve()
}

object ConfigBuilder {

  def build(customConfigs: Map[String, Any] = Map()): RuntimeConfig = {
    val runtimeConfig = new RuntimeConfig()
    customConfigs.foreach { case (key, value) => runtimeConfig.upsertConfig(key, value) }
    runtimeConfig
  }
}
