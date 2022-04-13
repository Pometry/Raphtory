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
  *   {s}`main(args: Array[String]): Unit` : Deploy
  *
  * ```{seealso}
  * [](com.raphtory.deployment.kubernetes.RaphtoryKubernetesNamespaces),
  * [](com.raphtory.deployment.kubernetes.RaphtoryKubernetesRegistrySecret),
  * [](com.raphtory.deployment.kubernetes.RaphtoryKubernetesServiceAccounts),
  * [](com.raphtory.deployment.kubernetes.RaphtoryKubernetesDeployments),
  * [](com.raphtory.deployment.kubernetes.RaphtoryKubernetesServices),
  * [](com.raphtory.deployment.kubernetes.RaphtoryKubernetesIngresses)
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
