package com.raphtory.deploy.kubernetes

import com.raphtory.deploy.kubernetes.components._

/** Calls `create` method on RaphtoryKubernetes classes to create kubernetes objects.
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
object Deploy {

  /** Deploy Kubernetes resources */
  def main(args: Array[String]): Unit = {
    RaphtoryKubernetesNamespaces.create()
    RaphtoryKubernetesRegistrySecret.create()
    RaphtoryKubernetesServiceAccounts.create()
    RaphtoryKubernetesDeployments.create()
    RaphtoryKubernetesServices.create()
    RaphtoryKubernetesIngresses.create()
  }
}
