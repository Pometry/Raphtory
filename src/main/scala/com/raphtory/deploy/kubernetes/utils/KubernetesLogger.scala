package com.raphtory.deploy.kubernetes.utils

import com.typesafe.scalalogging.LazyLogging

class KubernetesLogger extends LazyLogging {

  def info(string: String) =
    logger.info(string)

  def warn(string: String) =
    logger.warn(string)

  def error(msg: String, args: Any) =
    logger.error(msg, args)

  def debug(string: String) =
    logger.debug(string)
}

object KubernetesLogger {
  def apply(): KubernetesLogger = new KubernetesLogger()
}
