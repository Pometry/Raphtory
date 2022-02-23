package com.raphtory.deploy.kubernetes.components

import com.raphtory.core.deploy.Raphtory

import java.util
import com.raphtory.deploy.kubernetes.utils.KubernetesLogger
import com.typesafe.config

class Config {
  val conf: config.Config          = Raphtory.getDefaultConfig()
  val raphtoryDeploymentId: String = conf.getString("raphtory.deploy.id")

  val raphtoryKubernetesNamespaceName: String =
    conf.getString("raphtory.deploy.kubernetes.namespace.name")

  val raphtoryKubernetesServiceAccountName: String =
    conf.getString("raphtory.deploy.kubernetes.serviceaccount.name")

  val raphtoryKubernetesDeployments: util.Set[String] =
    conf.getConfig("raphtory.deploy.kubernetes.deployments").root().keySet()
  val raphtoryKubernetesMasterUrl: String             = conf.getString("raphtory.deploy.kubernetes.master.url")

  val raphtoryKubernetesLogger: KubernetesLogger =
    com.raphtory.deploy.kubernetes.utils.KubernetesLogger()

  val raphtoryKubernetesDockerRegistrySecretName: String =
    conf.getString("raphtory.deploy.kubernetes.secrets.registry.name")
}
