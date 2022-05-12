package com.raphtory.deployment.kubernetes.utils

import io.fabric8.kubernetes.api.model.IntOrString
import io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress
import io.fabric8.kubernetes.api.model.networking.v1beta1.IngressBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import scala.collection.JavaConverters._

/** Kubernetes Ingress   */
object KubernetesIngress {

  /**  Get Kubernetes ingress */
  def get(
      client: KubernetesClient,
      name: String,
      namespace: String
  ): Ingress =
    client.network.ingress.inNamespace(namespace).withName(name).get

  // TODO: Add logic to deal with hosts
  // TODO: Change object to better object that contains proper rule definition
  /** Build Kubernetes ingress */
  def build(
      name: String,
      annotations: Map[String, String] = Map(),
      path: String,
      backendServiceName: String,
      backendServicePort: Int
  ): Ingress =
    new IngressBuilder()
      .withNewMetadata()
      .withName(name)
      .addToAnnotations(annotations.asJava)
      .endMetadata()
      .withNewSpec()
      .addNewRule()
      .withNewHttp()
      .addNewPath()
      .withPath(path)
      .withNewBackend()
      .withServiceName(backendServiceName)
      .withServicePort(new IntOrString(backendServicePort))
      .endBackend()
      .endPath()
      .endHttp()
      .endRule()
      .endSpec()
      .build()

  /** Create Kubernetes ingress */
  def create(
      client: KubernetesClient,
      namespace: String,
      ingressConfig: Ingress
  ): Ingress =
    client.network().ingress().inNamespace(namespace).createOrReplace(ingressConfig)

  /** Delete Kubernetes ingress */
  def delete(
      client: KubernetesClient,
      namespace: String,
      name: String
  ): Boolean =
    client.network().ingress().inNamespace(namespace).withName(name).delete()
}
