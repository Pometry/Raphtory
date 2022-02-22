package com.raphtory.deploy.kubernetes

import com.raphtory.deploy.kubernetes.components._

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
