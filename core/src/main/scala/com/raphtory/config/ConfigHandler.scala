package com.raphtory.config

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValue
import com.typesafe.config.ConfigValueFactory
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/** @note DoNotDocument */
private[raphtory] class ConfigHandler {
  private lazy val defaults       = createConf()
  private lazy val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private lazy val deployedDistributed =
    defaults.resolve().getBoolean("raphtory.deploy.distributed")
  private val customConfigValues       = ArrayBuffer[(String, ConfigValue)]()

  private var salt = Random.nextInt().abs

  def addCustomConfig(path: String, value: Any) =
    customConfigValues += ((path, ConfigValueFactory.fromAnyRef(value)))

  def getConfig: Config =
    if (deployedDistributed)
      distributed()
    else
      local()

  def updateSalt(): Unit = {
    this.salt = Random.nextInt().abs
    logger.info(s"Raphtory deployment salt updated to '$salt'.")
  }

  def setSalt(salt: Int): Unit = {
    this.salt = salt
    logger.info(s"Raphtory deployment salt set to '$salt'.")
  }

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

  private def local(): Config = {
    val deploymentID = defaults.resolve().getString("raphtory.deploy.id") + "_" + salt
    val spoutTopic   = defaults.resolve().getString("raphtory.spout.topic") + "_" + salt
    defaults
      .withValue("raphtory.spout.topic", ConfigValueFactory.fromAnyRef(spoutTopic))
      .withValue("raphtory.deploy.id", ConfigValueFactory.fromAnyRef(deploymentID))
      .resolve()
  }

  private def distributed(): Config = defaults.resolve()
}
