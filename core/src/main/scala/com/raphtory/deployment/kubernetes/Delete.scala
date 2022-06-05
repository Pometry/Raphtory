package com.raphtory.deployment.kubernetes

import com.raphtory.deployment.kubernetes.components._

/** Calls `delete` method on RaphtoryKubernetes classes to delete kubernetes objects.
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
object Delete {

  /** Delete Kubernetes resources */
  def main(args: Array[String]): Unit = {
    RaphtoryKubernetesIngresses.delete
    RaphtoryKubernetesServices.delete
    RaphtoryKubernetesDeployments.delete
    RaphtoryKubernetesServiceAccounts.delete
    RaphtoryKubernetesNamespaces.delete
    RaphtoryKubernetesRegistrySecret.delete
  }
}
