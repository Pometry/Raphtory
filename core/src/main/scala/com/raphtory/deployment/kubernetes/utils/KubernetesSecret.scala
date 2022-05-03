package com.raphtory.deployment.kubernetes

import io.fabric8.kubernetes.api.model.Secret
import io.fabric8.kubernetes.api.model.SecretBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import scala.collection.JavaConverters._

/**
  * `KubernetesSecret`
  *
  * Kubernetes Secret
  *
  * ## Methods
  *
  *   `build(client: KubernetesClient, data: Map[String, String], name: String, secretType: String): Secret`
  *     : Build Kubernetes secret
  *
  *   `create(client: KubernetesClient, namespace: String, secret: Secret): Secret`
  *     : Create Kubernetes secret
  * 
  *   `delete(client: KubernetesClient, namespace: String, name: String): Boolean`
  *     : Delete Kubernetes secret
  */

object KubernetesSecret {

  def build(
      client: KubernetesClient,
      data: Map[String, String],
      name: String,
      secretType: String
  ): Secret =
    new SecretBuilder().withNewMetadata
      .withName(name)
      .endMetadata
      .addToData(data.asJava)
      .withType(secretType)
      .build

  def create(
      client: KubernetesClient,
      namespace: String,
      secret: Secret
  ): Secret =
    client.secrets.inNamespace(namespace).createOrReplace(secret)

  def delete(
      client: KubernetesClient,
      namespace: String,
      name: String
  ): Boolean =
    client.secrets.inNamespace(namespace).withName(name).delete()
}
