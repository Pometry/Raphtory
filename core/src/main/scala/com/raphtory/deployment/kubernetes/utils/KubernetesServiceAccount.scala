package com.raphtory.deployment.kubernetes.utils

import io.fabric8.kubernetes.api.model.ServiceAccount
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder
import io.fabric8.kubernetes.client.KubernetesClient

object KubernetesServiceAccount {

  def get(
      client: KubernetesClient,
      name: String
  ): ServiceAccount =
    client.serviceAccounts().withName(name).get

  def build(
      client: KubernetesClient,
      namespace: String,
      name: String
  ): ServiceAccount =
    new ServiceAccountBuilder().withNewMetadata().withName(name).endMetadata().build();

  def create(
      client: KubernetesClient,
      namespace: String,
      serviceAccountConfig: ServiceAccount
  ): ServiceAccount =
    client.serviceAccounts().inNamespace(namespace).createOrReplace(serviceAccountConfig)

  def delete(
      client: KubernetesClient,
      name: String
  ): Boolean =
    client.serviceAccounts.withName(name).delete
}
