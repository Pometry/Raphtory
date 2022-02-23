package com.raphtory.deploy.kubernetes.utils

import io.fabric8.kubernetes.api.model.apps.Deployment
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder
import io.fabric8.kubernetes.api.model.EnvVar
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.api.model.LocalObjectReference
import scala.collection.JavaConverters._

object KubernetesDeployment {

  def get(
      client: KubernetesClient,
      name: String,
      namespace: String
  ): Deployment =
    client.apps.deployments.inNamespace(namespace).withName(name).get

  def build(
      name: String,
      replicas: Int,
      labels: Map[String, String],
      annotations: Map[String, String] = Map(),
      containerName: String,
      containerImage: String,
      containerImagePullPolicy: String,
      containerPort: Int,
      matchLabels: Map[String, String],
      environmentVariables: Map[String, String],
      imagePullSecretsName: String
  ): Deployment = {

    val deploymentEnvVars = environmentVariables.map {
      case (key, value) => new EnvVar(key, value, null)
    }.toList

    new DeploymentBuilder()
      .withNewMetadata()
      .withName(name)
      .addToAnnotations(annotations.asJava)
      .endMetadata()
      .withNewSpec()
      .withReplicas(replicas)
      .withNewTemplate()
      .withNewMetadata()
      .addToLabels(labels.asJava)
      .endMetadata()
      .withNewSpec()
      .addNewContainer()
      .withName(containerName)
      .withImage(containerImage)
      .withImagePullPolicy(containerImagePullPolicy)
      .addNewPort()
      .withContainerPort(containerPort)
      .endPort()
      .withEnv(deploymentEnvVars.asJava)
      .endContainer()
      .addToImagePullSecrets(new LocalObjectReference(imagePullSecretsName))
      .endSpec()
      .endTemplate()
      .withNewSelector()
      .addToMatchLabels(matchLabels.asJava)
      .endSelector()
      .endSpec()
      .build()
  }

  def create(
      client: KubernetesClient,
      namespace: String,
      deploymentConfig: Deployment
  ): Deployment =
    client.apps().deployments().inNamespace(namespace).createOrReplace(deploymentConfig)

  def delete(
      client: KubernetesClient,
      namespace: String,
      name: String
  ): Boolean =
    client.apps().deployments().inNamespace(namespace).withName(name).delete();
}
