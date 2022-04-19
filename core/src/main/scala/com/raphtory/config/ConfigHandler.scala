package com.raphtory.config

import com.raphtory.graph.PerspectiveController.logger
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValue
import com.typesafe.config.ConfigValueFactory
import sun.util.logging.resources.logging

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/** @DoNotDocument */
private[raphtory] class ConfigHandler {
  private lazy val defaults = createConf()

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

  def updateSalt(): Unit =
    this.salt = Random.nextInt().abs

  def setSalt(salt: Int): Unit = {
    this.salt = salt
    logger.info(s"Raphtory deployment salt updated to '${salt}'.")
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
    logger.info(s"Raphtory deployment id set to '${defaults.getString("raphtory.deploy.id")}'.")
    defaults
      .withValue("raphtory.spout.topic", ConfigValueFactory.fromAnyRef(spoutTopic))
      .withValue("raphtory.deploy.id", ConfigValueFactory.fromAnyRef(deploymentID))
      .resolve()
  }

  private def distributed(): Config = defaults.resolve()
}
