package com.raphtory.deploy.kubernetes.components

import com.typesafe.config._
import com.raphtory.deploy.kubernetes.utils.KubernetesLogger
import java.util

/** Reads kubernetes configuration values from application.conf.
  */
class Config {
  var conf = ConfigFactory.load()

  val raphtoryDeploymentId: String = 
    conf.getString("raphtory.deploy.kubernetes.id")

  val raphtoryKubernetesNamespaceName: String =
    conf.getString("raphtory.deploy.kubernetes.namespace.name")

  val raphtoryKubernetesServiceAccountName: String =
    conf.getString("raphtory.deploy.kubernetes.serviceaccount.name")

  val raphtoryKubernetesDeployments: util.Set[String] =
    conf.getConfig("raphtory.deploy.kubernetes.deployments").root().keySet()

  val raphtoryKubernetesMasterUrl: String = 
    conf.getString("raphtory.deploy.kubernetes.master.url")

  val raphtoryKubernetesLogger: KubernetesLogger =
    KubernetesLogger()

  val raphtoryKubernetesDockerRegistrySecretName: String =
    conf.getString("raphtory.deploy.kubernetes.secrets.registry.name")
}
