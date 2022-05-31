package com.raphtory.deployment.kubernetes.components

import com.raphtory.deployment.Raphtory

import java.util
import com.raphtory.deployment.kubernetes.utils.KubernetesLogger
import com.typesafe.config

/** Reads kubernetes configuration values from application.conf.
  */
class Config {
  val conf: config.Config          = Raphtory.getDefaultConfig(distributed = true)
  val raphtoryDeploymentId: String = conf.getString("raphtory.deploy.id")

  val raphtoryKubernetesNamespaceName: String =
    conf.getString("raphtory.deploy.kubernetes.namespace.name")

  val raphtoryKubernetesServiceAccountName: String =
    conf.getString("raphtory.deploy.kubernetes.serviceaccount.name")

  val raphtoryKubernetesDeployments: util.Set[String] =
    conf.getConfig("raphtory.deploy.kubernetes.deployments").root().keySet()
  val raphtoryKubernetesMasterUrl: String             = conf.getString("raphtory.deploy.kubernetes.master.url")

  val raphtoryKubernetesLogger: KubernetesLogger =
    com.raphtory.deployment.kubernetes.utils.KubernetesLogger()

  val raphtoryKubernetesDockerRegistrySecretName: String =
    conf.getString("raphtory.deploy.kubernetes.secrets.registry.name")
}
