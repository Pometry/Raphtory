package com.raphtory.deployment.kubernetes

import com.raphtory.deployment.kubernetes.components._
/**
  * Kubernetes Deploy object
  *
  * Calls `create` method on RaphtoryKubernetes classes to create kubernetes objects.
  *
  * Kubernetes objects that are iterated over are read from application.conf values.
  *
  * ## Methods
  *
  *   `main(args: Array[String]): Unit`
  *     : Deploy Kubernetes resources
  *
  * ```{seealso}
  * [](com.raphtory.deployment.kubernetes.components.RaphtoryKubernetesNamespaces),
  * [](com.raphtory.deployment.kubernetes.components.RaphtoryKubernetesRegistrySecret),
  * [](com.raphtory.deployment.kubernetes.components.RaphtoryKubernetesServiceAccounts),
  * [](com.raphtory.deployment.kubernetes.components.RaphtoryKubernetesDeployments),
  * [](com.raphtory.deployment.kubernetes.components.RaphtoryKubernetesServices),
  * [](com.raphtory.deployment.kubernetes.components.RaphtoryKubernetesIngresses)
  * ```
  */

object Deploy {

  def main(args: Array[String]): Unit = {
    RaphtoryKubernetesNamespaces.create
    RaphtoryKubernetesRegistrySecret.create
    RaphtoryKubernetesServiceAccounts.create
    RaphtoryKubernetesDeployments.create
    RaphtoryKubernetesServices.create
    RaphtoryKubernetesIngresses.create
  }
}
