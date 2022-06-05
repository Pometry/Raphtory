package com.raphtory.deployment.kubernetes.utils

import io.fabric8.kubernetes.api.model.Namespace
import io.fabric8.kubernetes.api.model.NamespaceBuilder
import io.fabric8.kubernetes.client.KubernetesClient

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/** Kubernetes Namespace */
object KubernetesNamespace {

  /** Get Kubernetes namespace */
  def get(
      client: KubernetesClient,
      name: String
  ): Namespace =
    client.namespaces.withName(name).get

  /** Get a list of Kubernetes namespaces */
  def listAll(
      client: KubernetesClient
  ): ListBuffer[String] = {
    val namespaces = mutable.ListBuffer[String]()
    client.namespaces().list.getItems.forEach(x => namespaces += x.getMetadata.getName)
    namespaces
  }

  /** Create Kubernetes namespace */
  def create(
      client: KubernetesClient,
      name: String,
      labels: Map[String, String] = Map()
  ): Namespace = {
    val ns = new NamespaceBuilder().withNewMetadata
      .withName(name)
      .addToLabels(labels.asJava)
      .endMetadata
      .build

    client.namespaces().createOrReplace(ns)
  }

  /** Delete Kubernetes namespace */
  def delete(
      client: KubernetesClient,
      name: String
  ): Boolean =
    client.namespaces.withName(name).delete()
}
