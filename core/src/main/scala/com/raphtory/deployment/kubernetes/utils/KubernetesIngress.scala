package com.raphtory.deployment.kubernetes.utils

import io.fabric8.kubernetes.api.model.IntOrString
import io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress
import io.fabric8.kubernetes.api.model.networking.v1beta1.IngressBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import scala.collection.JavaConverters._

/**
  * {s}`KubernetesIngress`
  *
  * Kubernetes Ingress
  *
  * ## Methods
  *
  *   {s}`get(client: KubernetesClient, name: String, namespace: String): Ingress` : Get Kubernetes ingress
  *
  *   {s}`build(name: String, annotations: Map[String, String] = Map(), path: String, backendServiceName: String, backendServicePort: Int): Ingress` : Build Kubernetes ingress
  *
  *   {s}`create(client: KubernetesClient, namespace: String, ingressConfig: Ingress): Ingress` : Create Kubernetes ingress
  * 
  *   {s}`delete(client: KubernetesClient, namespace: String, name: String): Ingress` : Delete Kubernetes ingress
  */

object KubernetesIngress {

  def get(
      client: KubernetesClient,
      name: String,
      namespace: String
  ): Ingress =
    client.network.ingress.inNamespace(namespace).withName(name).get

  // TODO: Add logic to deal with hosts
  // TODO: Change object to better object that contains proper rule definition
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

  def create(
      client: KubernetesClient,
      namespace: String,
      ingressConfig: Ingress
  ): Ingress =
    client.network().ingress().inNamespace(namespace).createOrReplace(ingressConfig)

  def delete(
      client: KubernetesClient,
      namespace: String,
      name: String
  ): Boolean =
    client.network().ingress().inNamespace(namespace).withName(name).delete()
}
