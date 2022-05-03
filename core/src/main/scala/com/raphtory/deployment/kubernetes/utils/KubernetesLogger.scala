package com.raphtory.deployment.kubernetes.utils

import com.typesafe.scalalogging.LazyLogging

/**
  * `KubernetesLogger`
  *
  * Kubernetes Logger
  *
  * ## Methods
  *
  *   `apply()` : create new Kubernetes Logger
  *
  *   `info(string: String)`
  *     : Log info message
  *
  *   `warn(string: String)`
  *     : Log warn message
  *
  *   `error(string: String)`
  *     : Log error message
  * 
  *   `debug(string: String)`
  *     : Log debug message
  */

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
