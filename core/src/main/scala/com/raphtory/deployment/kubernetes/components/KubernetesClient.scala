package com.raphtory.deployment.kubernetes.components

import io.fabric8.kubernetes.client.ConfigBuilder
import io.fabric8.kubernetes.client.DefaultKubernetesClient

/** Establishes kubernetes connection. Extends Config.
  * Kubernetes configuration values are read from application.conf values.
  * @see [[com.raphtory.deployment.kubernetes.components.Config]]
  */
class KubernetesClient extends Config {

  val kubernetesClient = new DefaultKubernetesClient(
          new ConfigBuilder().withMasterUrl(raphtoryKubernetesMasterUrl).build()
  )
}
