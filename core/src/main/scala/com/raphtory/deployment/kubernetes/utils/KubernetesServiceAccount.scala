package com.raphtory.deployment.kubernetes.utils

import io.fabric8.kubernetes.api.model.ServiceAccount
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder
import io.fabric8.kubernetes.client.KubernetesClient

/**
  * {s}`KubernetesServiceAccount`
  *
  * Kubernetes Service Account
  *
  * ## Methods
  *
  *   {s}`get(client: KubernetesClient, name: String): ServiceAccount`
  *     : Get Kubernetes service account
  *
  *   {s}`build( client: KubernetesClient, namespace: String, name: String): ServiceAccount` 
  *     : Build Kubernetes service account
  *
  *   {s}`create( client: KubernetesClient, namespace: String, serviceAccountConfig: ServiceAccount): ServiceAccount
  *     : Create Kubernetes service account
  * 
  *   {s}`delete( client: KubernetesClient, name: String): Boolean`
  *     : Delete Kubernetes service account
  */

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
