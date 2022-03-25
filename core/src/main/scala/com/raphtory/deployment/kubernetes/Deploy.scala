package com.raphtory.deployment.kubernetes

import com.raphtory.deployment.kubernetes.components._

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
