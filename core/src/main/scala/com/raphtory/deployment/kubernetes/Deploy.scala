package com.raphtory.deployment.kubernetes

import com.raphtory.deployment.kubernetes.components._

/** Calls `create` method on RaphtoryKubernetes classes to create kubernetes objects.
  * Kubernetes objects that are iterated over are read from application.conf values.
  *
  * @see
  * [[com.raphtory.deployment.kubernetes.components.RaphtoryKubernetesNamespaces]]
  * [[com.raphtory.deployment.kubernetes.components.RaphtoryKubernetesRegistrySecret]]
  * [[com.raphtory.deployment.kubernetes.components.RaphtoryKubernetesServiceAccounts]]
  * [[com.raphtory.deployment.kubernetes.components.RaphtoryKubernetesDeployments]]
  * [[com.raphtory.deployment.kubernetes.components.RaphtoryKubernetesServices]]
  * [[com.raphtory.deployment.kubernetes.components.RaphtoryKubernetesIngresses]]
  */
object Deploy {

  /** Deploy Kubernetes resources */
  def main(args: Array[String]): Unit = {
    RaphtoryKubernetesNamespaces.create
    RaphtoryKubernetesRegistrySecret.create
    RaphtoryKubernetesServiceAccounts.create
    RaphtoryKubernetesDeployments.create
    RaphtoryKubernetesServices.create
    RaphtoryKubernetesIngresses.create
  }
}
