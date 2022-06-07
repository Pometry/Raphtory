package com.raphtory.deploy.kubernetes.utils

import com.typesafe.scalalogging.LazyLogging

/** Kubernetes Logger */
class KubernetesLogger extends LazyLogging {

  /** Log info message */
  def info(string: String) =
    logger.info(string)

  /** Log warn message */
  def warn(string: String) =
    logger.warn(string)

  /** Log error message */
  def error(msg: String, args: Any) =
    logger.error(msg, args)

  /** Log debug message */
  def debug(string: String) =
    logger.debug(string)
}

/** Kubernetes Logger */
object KubernetesLogger {

  /** create new Kubernetes Logger */
  def apply(): KubernetesLogger = new KubernetesLogger()
}
