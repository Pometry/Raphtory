package com.raphtory.deployment.kubernetes.components

import com.raphtory.deployment.kubernetes.utils.KubernetesSecret

import java.util.Base64

/** Extends KubernetesClient which extends Config.
  * KubernetesClient is used to establish kubernetes connection.
  * Kubernetes object config is read from application.conf values.
  *
  * @see [[com.raphtory.deployment.kubernetes.components.Config]]
  * [[com.raphtory.deployment.kubernetes.components.KubernetesClient]]
  */
object RaphtoryKubernetesRegistrySecret extends KubernetesClient {

  /** Create kubernetes registry secrets needed for Raphtory (if toggled in application.conf) */
  def create(): Unit =
    if (
            conf.hasPath("raphtory.deploy.kubernetes.secrets.registry.create") &&
            conf.getBoolean("raphtory.deploy.kubernetes.secrets.registry.create")
    ) {
      raphtoryKubernetesLogger.info(
              s"Deploying $raphtoryKubernetesDockerRegistrySecretName secret into $raphtoryKubernetesNamespaceName namespace"
      )

      val server     = conf.getString("raphtory.deploy.kubernetes.secrets.registry.server")
      val username   = conf.getString("raphtory.deploy.kubernetes.secrets.registry.username")
      val password   = conf.getString("raphtory.deploy.kubernetes.secrets.registry.password")
      val email      = conf.getString("raphtory.deploy.kubernetes.secrets.registry.email")
      val authkey    = Base64.getEncoder.encodeToString(s"$username:$password".getBytes()).trim()
      val authstring =
        s"""{"auths":{"$server":{"username":"$username","password":"$password","email":"$email","auth":"$authkey"}}}"""
      val dockercfg  = Base64.getEncoder.encodeToString(s"$authstring".getBytes()).trim()

      try KubernetesSecret.create(
              client = kubernetesClient,
              namespace = raphtoryKubernetesNamespaceName,
              secret = KubernetesSecret.build(
                      client = kubernetesClient,
                      name = raphtoryKubernetesDockerRegistrySecretName,
                      secretType = "kubernetes.io/dockerconfigjson",
                      data = Map(".dockerconfigjson" -> dockercfg)
              )
      )
      catch {
        case e: Throwable =>
          raphtoryKubernetesLogger.error(
                  s"Error found when deploying $raphtoryKubernetesDockerRegistrySecretName secret into $raphtoryKubernetesNamespaceName namespace",
                  e
          )
      }
    }
    else
      raphtoryKubernetesLogger.info(
              s"Setting raphtory.deploy.kubernetes.secrets.registry.create is set to false"
      )

  /** Delete kubernetes registry secrets needed for Raphtory (if toggled in application.conf) */
  def delete(): Unit =
    if (
            conf.hasPath("raphtory.deploy.kubernetes.secrets.registry.delete") &&
            conf.getBoolean("raphtory.deploy.kubernetes.secrets.registry.delete")
    ) {
      raphtoryKubernetesLogger.info(
              s"Deleting $raphtoryKubernetesDockerRegistrySecretName secret from $raphtoryKubernetesNamespaceName namespace"
      )

      try KubernetesSecret.delete(
              client = kubernetesClient,
              namespace = raphtoryKubernetesNamespaceName,
              name = raphtoryKubernetesDockerRegistrySecretName
      )
      catch {
        case e: Throwable =>
          raphtoryKubernetesLogger.error(
                  s"Error found when deleting $raphtoryKubernetesDockerRegistrySecretName secret from $raphtoryKubernetesNamespaceName namespace",
                  e
          )
      }
    }
    else
      raphtoryKubernetesLogger.info(
              s"Setting raphtory.deploy.kubernetes.secrets.registry.delete is set to false"
      )
}
