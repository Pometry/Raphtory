package com.raphtory.deploy.kubernetes.components

import io.fabric8.kubernetes.client.ConfigBuilder
import io.fabric8.kubernetes.client.DefaultKubernetesClient

class KubernetesClient extends Config {

  val kubernetesClient = new DefaultKubernetesClient(
          new ConfigBuilder().withMasterUrl(raphtoryKubernetesMasterUrl).build()
  )
}
