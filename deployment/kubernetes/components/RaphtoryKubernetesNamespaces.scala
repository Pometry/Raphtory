package com.raphtory.deploy.kubernetes.components

import com.raphtory.deploy.kubernetes.utils.KubernetesNamespace

object RaphtoryKubernetesNamespaces extends KubernetesClient {

  def create(): Unit =
    if (
            conf.hasPath("raphtory.deploy.kubernetes.namespace.create") &&
            conf.getBoolean("raphtory.deploy.kubernetes.namespace.create")
    ) {
      raphtoryKubernetesLogger.info(s"Deploying $raphtoryKubernetesNamespaceName namespace")

      try KubernetesNamespace.create(
              client = kubernetesClient,
              name = raphtoryKubernetesNamespaceName,
              labels = Map("deployment" -> "raphtory")
      )
      catch {
        case e: Throwable =>
          raphtoryKubernetesLogger.error(
                  s"Error found when deploying $raphtoryKubernetesNamespaceName namespace",
                  e
          )
      }
    }
    else
      raphtoryKubernetesLogger.info(
              s"Setting raphtory.deploy.kubernetes.namespace.create is set to false"
      )

  def delete(): Unit =
    if (
            conf.hasPath("raphtory.deploy.kubernetes.namespace.delete") &&
            conf.getBoolean("raphtory.deploy.kubernetes.namespace.delete")
    ) {

      val namespace =
        try Option(
                KubernetesNamespace.get(
                        client = kubernetesClient,
                        name = raphtoryKubernetesNamespaceName
                )
        )
        catch {
          case e: Throwable =>
            raphtoryKubernetesLogger.error(
                    s"Error found when getting $raphtoryKubernetesNamespaceName namespace",
                    e
            )
        }

      namespace match {
        case None        =>
          raphtoryKubernetesLogger.debug(
                  s"Namespace $raphtoryKubernetesNamespaceName not found. Delete aborted"
          )
        case Some(value) =>
          raphtoryKubernetesLogger.info(
                  s"Namespace $raphtoryKubernetesNamespaceName found. Deleting"
          )
          try KubernetesNamespace.delete(
                  client = kubernetesClient,
                  name = raphtoryKubernetesNamespaceName
          )
          catch {
            case e: Throwable =>
              raphtoryKubernetesLogger.error(
                      s"Error found when deleting $raphtoryKubernetesNamespaceName namespace",
                      e
              )
          }
      }
    }
    else
      raphtoryKubernetesLogger.info(
              s"Setting raphtory.deploy.kubernetes.namespace.delete is set to false"
      )
}
