package com.raphtory.deploy.kubernetes

import com.raphtory.deploy.kubernetes.components._

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
