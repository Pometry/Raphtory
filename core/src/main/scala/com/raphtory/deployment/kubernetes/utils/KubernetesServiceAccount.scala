package com.raphtory.deployment.kubernetes.utils

import io.fabric8.kubernetes.api.model.ServiceAccount
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder
import io.fabric8.kubernetes.client.KubernetesClient

/** Kubernetes Service Account */
object KubernetesServiceAccount {

  /** Get Kubernetes service account */
  def get(
      client: KubernetesClient,
      name: String
  ): ServiceAccount =
    client.serviceAccounts().withName(name).get

  /** Build Kubernetes service account */
  def build(
      client: KubernetesClient,
      namespace: String,
      name: String
  ): ServiceAccount =
    new ServiceAccountBuilder().withNewMetadata().withName(name).endMetadata().build();

  /**  Create Kubernetes service account */
  def create(
      client: KubernetesClient,
      namespace: String,
      serviceAccountConfig: ServiceAccount
  ): ServiceAccount =
    client.serviceAccounts().inNamespace(namespace).createOrReplace(serviceAccountConfig)

  /** Delete Kubernetes service account */
  def delete(
      client: KubernetesClient,
      name: String
  ): Boolean =
    client.serviceAccounts.withName(name).delete
}
