package com.raphtory.deployment.kubernetes.utils

import com.typesafe.scalalogging.LazyLogging

/**
  * {s}`KubernetesLogger`
  *
  * Kubernetes Logger
  *
  * ## Methods
  *
  *   {s}`apply()` : create new Kubernetes Logger
  *
  *   {s}`info(string: String)` 
  *     : Log info message
  *
  *   {s}`warn(string: String)`
  *     : Log warn message
  *
  *   {s}`error(string: String)`
  *     : Log error message
  * 
  *   {s}`debug(string: String)`
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
