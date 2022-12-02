package com

import cats.effect.Resource
import cats.effect.Sync
import com.oblac.nomen.Nomen
import com.raphtory.internals.management.GraphConfig.ConfigBuilder
import com.raphtory.internals.management.id.LocalIDManager
import com.typesafe.config.Config

package object raphtory {

  private[raphtory] def createName: String = Nomen.est().adjective().color().animal().get()

  private[raphtory] val defaultConf: Config  = ConfigBuilder.getDefaultConfig
  private[raphtory] lazy val deployInterface = defaultConf.getString("raphtory.deploy.address")
  private[raphtory] lazy val deployPort      = defaultConf.getInt("raphtory.deploy.port")
}
