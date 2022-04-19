package com.raphtory.deployment.kubernetes.utils

import io.fabric8.kubernetes.api.model.apps.{Deployment, DeploymentBuilder}
import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.client.KubernetesClient
import com.typesafe.config.Config
import scala.collection.JavaConverters._

/**
  * {s}`KubernetesDeployment`
  *
  * Kubernetes deployment
  *
  * ## Methods
  *
  *   {s}`get(client: KubernetesClient, name: String, namespace: String): Deployment`
  *     : Get Kubernetes deployment
  *
  *   {s}`build(name: String, replicas: Int, labels: Map[String, String], annotations: Map[String, String] = Map(), containerName: String, containerImage: String, containerImagePullPolicy: String, containerPort: Int, matchLabels: Map[String, String], environmentVariables: Map[String, String], imagePullSecretsName: String, resources: Config, affinity: Config, antiAffinity: Config): Deployment`
  *     : Build Kubernetes deployment
  *
  *   {s}`create(client: KubernetesClient, namespace: String, deploymentConfig: Deployment): Deployment`
  *     : Create Kubernetes deployment
  * 
  *   {s}`delete(client: KubernetesClient, namespace: String, name: String): Boolean`
  *     : Delete Kubernetes deployment
  */

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
      imagePullSecretsName: String,
      resources: Config,
      affinity: Config,
      antiAffinity: Config
  ): Deployment = {

    // Build environment variables to use in deployment
    val deploymentEnvVars = environmentVariables.map {
      case (key, value) => new EnvVar(key, value, null)
    }.toList

    // Build resources to use in deployment
    val resourceRequirements = new ResourceRequirementsBuilder().build()

    // Add requests if defined
    if (resources.hasPath("requests")) {
      if (resources.hasPath("requests.memory")) {
        resourceRequirements.setRequests(Map("memory" -> new QuantityBuilder()
            .withAmount(resources.getString("requests.memory.amount"))
            .withFormat(resources.getString("requests.memory.format"))
            .build
          ).asJava
        )
      }
      if (resources.hasPath("requests.cpu")) {
        resourceRequirements.setRequests(Map("cpu" -> new QuantityBuilder()
            .withAmount(resources.getString("requests.cpu.amount"))
            .withFormat(resources.getString("requests.cpu.format"))
            .build
          ).asJava
        )
      }
    }

    // Add limits if defined
    if (resources.hasPath("limits")) {
      if (resources.hasPath("limits.memory")) {
        resourceRequirements.setRequests(Map("memory" -> new QuantityBuilder()
            .withAmount(resources.getString("limits.memory.amount"))
            .withFormat(resources.getString("limits.memory.format"))
            .build
          ).asJava
        )
      }
      if (resources.hasPath("limits.cpu")) {
        resourceRequirements.setRequests(Map("cpu" -> new QuantityBuilder()
            .withAmount(resources.getString("limits.cpu.amount"))
            .withFormat(resources.getString("limits.cpu.format"))
            .build
          ).asJava
        )
      }
    }

    // Build Anti Affinity Term
    val podAffinity = new AffinityBuilder().build()

    if (affinity.getBoolean("enabled")) {
      println("Affinity enabled")
    }
    if (antiAffinity.getBoolean("enabled")) {
      println("Anti affinity enabled")
    }
    //  val podAntiAffinityTerm = new PodAffinityTermBuilder()
    //    .withTopologyKey(podAntiAffinityTermTopologyKey)
    //    .withNewLabelSelector()
    //    .withMatchLabels(podAntiAffinityTermMatchLabels.asJava)
    //    .endLabelSelector()
    //    .build();
//
    //  podAntiAffinityTerm.setNamespaceSelector(new LabelSelector())
    //  podAffinity.setPodAntiAffinity(
    //    new PodAntiAffinityBuilder()
    //      .withRequiredDuringSchedulingIgnoredDuringExecution(podAntiAffinityTerm)
    //      .build())
    //}

    // Build Affinity Term
    //if (!podAffinityTermMatchLabels.isEmpty) {
    //  val podAffinityTerm = new PodAffinityTermBuilder()
    //    .withTopologyKey(podAffinityTermTopologyKey)
    //    .withNewLabelSelector()
    //    .withMatchLabels(podAffinityTermMatchLabels.asJava)
    //    .endLabelSelector()
    //    .build();
//
    //  podAffinityTerm.setNamespaceSelector(new LabelSelector())
    //  podAffinity.setPodAffinity(
    //    new PodAffinityBuilder()
    //      .withRequiredDuringSchedulingIgnoredDuringExecution(podAffinityTerm)
    //      .build())
    //}


    // Build Affinity rules for use in deployment
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
      .withAffinity(podAffinity)
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
