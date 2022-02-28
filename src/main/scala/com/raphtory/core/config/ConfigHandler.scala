package com.raphtory.core.config

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValue
import com.typesafe.config.ConfigValueFactory

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/** @DoNotDocument */
private[core] class ConfigHandler {
  private var salt                     = Random.nextInt().abs
  private lazy val defaults            = createConf()
  private lazy val deployedDistributed = defaults.getBoolean("raphtory.deploy.distributed")
  private val customConfigValues       = ArrayBuffer[(String, ConfigValue)]()

  def addCustomConfig(path: String, value: Any) =
    customConfigValues += ((path, ConfigValueFactory.fromAnyRef(value)))

  def get(): Config =
    if (deployedDistributed)
      distributed()
    else
      local()

  private def createConf(): Config = {
    var tempConf = ConfigFactory.defaultOverrides().withFallback(ConfigFactory.defaultApplication())
    customConfigValues.foreach {
      case (path, value) =>
        tempConf = tempConf.withValue(path, ConfigValueFactory.fromAnyRef(value))
    }
    tempConf.resolve()
  }

  private def local(): Config = {
    val deploymentID = defaults.getString("raphtory.deploy.id") + "_" + salt
    val spoutTopic   = defaults.getString("raphtory.spout.topic") + "_" + salt
    ConfigFactory
      .defaultOverrides()
      .withFallback(ConfigFactory.defaultApplication())
      .withValue("raphtory.spout.topic", ConfigValueFactory.fromAnyRef(spoutTopic))
      .withValue("raphtory.deploy.id", ConfigValueFactory.fromAnyRef(deploymentID))
      .resolve()
  }

  private def distributed(): Config =
    ConfigFactory.defaultOverrides().withFallback(ConfigFactory.defaultApplication()).resolve()

  def updateSalt(): Unit =
    salt = Random.nextInt().abs
}
