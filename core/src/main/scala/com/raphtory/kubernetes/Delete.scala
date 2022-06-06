package com.raphtory.kubernetes

import com.raphtory.kubernetes.components.RaphtoryKubernetesDeployments
import com.raphtory.kubernetes.components.RaphtoryKubernetesIngresses
import com.raphtory.kubernetes.components.RaphtoryKubernetesNamespaces
import com.raphtory.kubernetes.components.RaphtoryKubernetesRegistrySecret
import com.raphtory.kubernetes.components.RaphtoryKubernetesServiceAccounts
import com.raphtory.kubernetes.components.RaphtoryKubernetesServices

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
