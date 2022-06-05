package com.raphtory.deployment.kubernetes.utils

import io.fabric8.kubernetes.api.model.Secret
import io.fabric8.kubernetes.api.model.SecretBuilder
import io.fabric8.kubernetes.client.KubernetesClient

import scala.jdk.CollectionConverters._

/** Kubernetes Secret */
object KubernetesSecret {

  /** Build Kubernetes secret */
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

  /** Create Kubernetes secret */
  def create(
      client: KubernetesClient,
      namespace: String,
      secret: Secret
  ): Secret =
    client.secrets.inNamespace(namespace).createOrReplace(secret)

  /** Delete Kubernetes secret */
  def delete(
      client: KubernetesClient,
      namespace: String,
      name: String
  ): Boolean =
    client.secrets.inNamespace(namespace).withName(name).delete()
}
