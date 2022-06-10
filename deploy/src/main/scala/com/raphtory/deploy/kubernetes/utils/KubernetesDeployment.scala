package com.raphtory.deploy.kubernetes.utils

import com.typesafe.config.Config
import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.api.model.apps.{Deployment, DeploymentBuilder}
import io.fabric8.kubernetes.client.KubernetesClient
import scala.jdk.CollectionConverters._

/** Kubernetes deployment */
object KubernetesDeployment {

  /** Get Kubernetes deployment */
  def get(
      client: KubernetesClient,
      name: String,
      namespace: String
  ): Deployment =
    client.apps.deployments.inNamespace(namespace).withName(name).get

  /** Build Kubernetes deployment */
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
      imagePullSecretsName: String,
      resources: Config
  ): Deployment = {

    // Build environment variables to use in deployment
    val deploymentEnvVars = environmentVariables.map {
      case (key, value) => new EnvVar(key, value, null)
    }.toList

    // Build resources to use in deployment
    val resourceRequirements = new ResourceRequirementsBuilder().build()

    // Add requests if defined
    if (resources.hasPath("requests")) {
      if (resources.hasPath("requests.memory"))
        resourceRequirements.setRequests(
                Map(
                        "memory" -> new QuantityBuilder()
                          .withAmount(resources.getString("requests.memory.amount"))
                          .withFormat(resources.getString("requests.memory.format"))
                          .build
                ).asJava
        )
      if (resources.hasPath("requests.cpu"))
        resourceRequirements.setRequests(
                Map(
                        "cpu" -> new QuantityBuilder()
                          .withAmount(resources.getString("requests.cpu.amount"))
                          .withFormat(resources.getString("requests.cpu.format"))
                          .build
                ).asJava
        )
    }

    // Add limits if defined
    if (resources.hasPath("limits")) {
      if (resources.hasPath("limits.memory"))
        resourceRequirements.setRequests(
                Map(
                        "memory" -> new QuantityBuilder()
                          .withAmount(resources.getString("limits.memory.amount"))
                          .withFormat(resources.getString("limits.memory.format"))
                          .build
                ).asJava
        )
      if (resources.hasPath("limits.cpu"))
        resourceRequirements.setRequests(
                Map(
                        "cpu" -> new QuantityBuilder()
                          .withAmount(resources.getString("limits.cpu.amount"))
                          .withFormat(resources.getString("limits.cpu.format"))
                          .build
                ).asJava
        )
    }

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
      .withResources(resourceRequirements)
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

  /** Create Kubernetes deployment */
  def create(
      client: KubernetesClient,
      namespace: String,
      deploymentConfig: Deployment
  ): Deployment =
    client.apps().deployments().inNamespace(namespace).createOrReplace(deploymentConfig)

  /** Delete Kubernetes deployment */
  def delete(
      client: KubernetesClient,
      namespace: String,
      name: String
  ): Boolean =
    client.apps().deployments().inNamespace(namespace).withName(name).delete();
}
