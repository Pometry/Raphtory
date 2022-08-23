package com.raphtory.internals.management

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValue
import com.typesafe.config.ConfigValueFactory
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

private[raphtory] class ConfigHandler {
  private lazy val defaults       = createConf()
  private lazy val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private val customConfigValues = ArrayBuffer[(String, ConfigValue)]()

  private var salt = Random.nextInt().abs

  def addCustomConfig(path: String, value: Any) =
    customConfigValues += ((path, ConfigValueFactory.fromAnyRef(value)))

  def getConfig(): Config = defaults.resolve()

  private def createConf(): Config = {
    var tempConf = ConfigFactory
      .defaultOverrides()
      .withFallback(
              ConfigFactory.defaultApplication()
      )

    customConfigValues.foreach {
      case (path, value) =>
        tempConf = tempConf
          .withValue(
                  path,
                  ConfigValueFactory.fromAnyRef(value)
          )
    }

    tempConf
  }
}
