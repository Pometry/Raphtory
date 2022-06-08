package com.raphtory.deploy.kubernetes.components

import io.fabric8.kubernetes.client.ConfigBuilder
import io.fabric8.kubernetes.client.DefaultKubernetesClient

/** Establishes kubernetes connection. Extends Config.
  * Kubernetes configuration values are read from application.conf values.
  * @see [[Config]]
  */
class KubernetesClient extends Config {

  val kubernetesClient = new DefaultKubernetesClient(
          new ConfigBuilder().withMasterUrl(raphtoryKubernetesMasterUrl).build()
  )
}
