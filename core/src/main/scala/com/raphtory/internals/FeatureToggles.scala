package com.raphtory.internals

import com.typesafe.config.{Config, ConfigFactory}

object FeatureToggles {
  val conf: Config = ConfigFactory.load

  val isFlightEnabled: Boolean = conf.getBoolean("raphtory.featureToggle.flight")

}
