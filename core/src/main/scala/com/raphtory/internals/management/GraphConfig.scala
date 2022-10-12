package com.raphtory.internals.management

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValueFactory

private[raphtory] class GraphConfig private(config: Config) {
  def getConfig: Config = config
}

object GraphConfig {

  case class ConfigBuilder() {
    var tempConf: Config =
      ConfigFactory
        .defaultOverrides()
        .withFallback(ConfigFactory.defaultApplication())

    def addConfig(key: String, value: Any): ConfigBuilder = {
      tempConf = tempConf
        .withValue(
                key,
                ConfigValueFactory.fromAnyRef(value)
        )
      this
    }

    def build(): GraphConfig = new GraphConfig(tempConf.resolve())
  }
}
