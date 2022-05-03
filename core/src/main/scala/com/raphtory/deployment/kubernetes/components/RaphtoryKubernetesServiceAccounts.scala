package com.raphtory.deployment.kubernetes.components

import com.raphtory.deployment.kubernetes.utils.KubernetesServiceAccount

/**
  * `RaphtoryKubernetesServiceAccounts`
  *
  * Extends KubernetesClient which extends Config.
  *
  * KubernetesClient is used to establish kubernetes connection.
  * 
  * Kubernetes object config is read from application.conf values.
  *
  * ## Methods
  *
  *   `create(): Unit`
  *     : Create kubernetes service accounts needed for Raphtory (if toggled in application.conf)
  *
  *   `delete(): Unit`
  *     : Delete kubernetes service accounts needed for Raphtory (if toggled in application.conf)
  *
  * ```{seealso}
  * [](com.raphtory.deployment.kubernetes.components.Config),
  * [](com.raphtory.deployment.kubernetes.components.KubernetesClient),
  * [](com.raphtory.deployment.kubernetes.utils.KubernetesServiceAccount)
  * ```
  */

object RaphtoryKubernetesServiceAccounts extends KubernetesClient {

  def create(): Unit =
    if (
            conf.hasPath("raphtory.deploy.kubernetes.serviceaccount.create") &&
            conf.getBoolean("raphtory.deploy.kubernetes.serviceaccount.create")
    ) {
      raphtoryKubernetesLogger.info(
              s"Deploying service account $raphtoryKubernetesServiceAccountName"
      )

      try KubernetesServiceAccount.create(
              client = kubernetesClient,
              namespace = raphtoryKubernetesNamespaceName,
              serviceAccountConfig = KubernetesServiceAccount.build(
                      client = kubernetesClient,
                      namespace = raphtoryKubernetesNamespaceName,
                      name = raphtoryKubernetesServiceAccountName
              )
      )
      catch {
        case e: Throwable =>
          raphtoryKubernetesLogger.error(
                  s"Error found when deploying KubernetesServiceAccounts for $raphtoryKubernetesServiceAccountName service account",
                  e
          )
      }
    }
    else
      raphtoryKubernetesLogger.info(
              s"Setting raphtory.deploy.kubernetes.serviceaccount.create is set to false"
      )

  def delete(): Unit =
    if (
            conf.hasPath("raphtory.deploy.kubernetes.serviceaccount.delete") &&
            conf.getBoolean("raphtory.deploy.kubernetes.serviceaccount.delete")
    ) {
      val serviceAccount =
        try Option(
                KubernetesServiceAccount.get(
                        client = kubernetesClient,
                        name = raphtoryKubernetesServiceAccountName
                )
        )
        catch {
          case e: Throwable =>
            raphtoryKubernetesLogger.error(
                    s"Error found when getting KubernetesServiceAccount for $raphtoryKubernetesServiceAccountName",
                    e
            )
        }

      serviceAccount match {
        case None        =>
          raphtoryKubernetesLogger.debug(
                  s"Service account not found for $raphtoryKubernetesServiceAccountName. Delete aborted"
          )
        case Some(value) =>
          raphtoryKubernetesLogger.info(
                  s"Service account found for $raphtoryKubernetesServiceAccountName. Deleting"
          )
          try KubernetesServiceAccount.delete(
                  client = kubernetesClient,
                  name = raphtoryKubernetesServiceAccountName
          )
          catch {
            case e: Throwable =>
              raphtoryKubernetesLogger.error(
                      s"Error found when deleting KubernetesServiceAccount for $raphtoryKubernetesServiceAccountName service account",
                      e
              )
          }
      }
    }
    else
      raphtoryKubernetesLogger.info(
              s"Setting raphtory.deploy.kubernetes.serviceaccount.delete is set to false"
      )
}
