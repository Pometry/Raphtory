package com.raphtory.deployment.kubernetes

import com.raphtory.deployment.kubernetes.components._

/**
  * Kubernetes Delete object
  *
  * Calls `delete` method on RaphtoryKubernetes classes to delete kubernetes objects.
  * 
  * Kubernetes objects that are iterated over are read from application.conf values. 
  *
  * ## Methods
  *
  *   {s}`main(args: Array[String]): Unit` : Delete
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

object Delete {

  def main(args: Array[String]): Unit = {
    RaphtoryKubernetesIngresses.delete
    RaphtoryKubernetesServices.delete
    RaphtoryKubernetesDeployments.delete
    RaphtoryKubernetesServiceAccounts.delete
    RaphtoryKubernetesNamespaces.delete
    RaphtoryKubernetesRegistrySecret.delete
  }
}
