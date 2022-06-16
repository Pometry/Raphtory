package com.raphtory.deploy.kubernetes

import components._

/** Calls `delete` method on RaphtoryKubernetes classes to delete kubernetes objects.
  * Kubernetes objects that are iterated over are read from application.conf values.
  *
  * @see
  * [[RaphtoryKubernetesNamespaces]]
  * [[RaphtoryKubernetesRegistrySecret]]
  * [[RaphtoryKubernetesServiceAccounts]]
  * [[RaphtoryKubernetesDeployments]]
  * [[RaphtoryKubernetesServices]]
  * [[RaphtoryKubernetesIngresses]]
  */
object Delete {

  /** Delete Kubernetes resources */
  def main(args: Array[String]): Unit = {
    RaphtoryKubernetesIngresses.delete()
    RaphtoryKubernetesServices.delete()
    RaphtoryKubernetesDeployments.delete()
    RaphtoryKubernetesServiceAccounts.delete()
    RaphtoryKubernetesNamespaces.delete()
    RaphtoryKubernetesRegistrySecret.delete()
  }
}
