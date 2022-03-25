package com.raphtory.deployment.kubernetes

import com.raphtory.deployment.kubernetes.components._

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
