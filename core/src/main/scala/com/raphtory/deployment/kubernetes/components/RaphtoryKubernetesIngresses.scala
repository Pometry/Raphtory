package com.raphtory.deployment.kubernetes.components

import com.raphtory.deployment.kubernetes.utils.KubernetesIngress
import com.raphtory.deployment.kubernetes.utils.KubernetesService

/**
  * `RaphtoryKubernetesIngresses`
  *
  * Extends KubernetesClient which extends Config.
  *
  * KubernetesClient is used to establish kubernetes connection.
  * 
  * Kubernetes objects that are iterated over are read from application.conf values.
  *
  * ## Methods
  *
  *   `create(): Unit`
  *     : Create kubernetes ingresses needed for Raphtory (if toggled in application.conf)
  *
  *   `delete(): Unit`
  *     : Delete kubernetes ingresses needed for Raphtory (if toggled in application.conf)
  *
  * ```{seealso}
  * [](com.raphtory.deployment.kubernetes.components.Config),
  * [](com.raphtory.deployment.kubernetes.components.KubernetesClient),
  * [](com.raphtory.deployment.kubernetes.utils.KubernetesIngress),
  * [](com.raphtory.deployment.kubernetes.utils.KubernetesService)
  * ```
  */

object RaphtoryKubernetesIngresses extends KubernetesClient {

  def create(): Unit =
    raphtoryKubernetesDeployments.forEach { raphtoryComponent =>
      if (
              conf.hasPath(
                      s"raphtory.deploy.kubernetes.deployments.$raphtoryComponent.ingress.create"
              ) &&
              conf.getBoolean(
                      s"raphtory.deploy.kubernetes.deployments.$raphtoryComponent.ingress.create"
              )
      ) {
        val serviceName = s"raphtory-$raphtoryDeploymentId-$raphtoryComponent-svc".toLowerCase()
        val service     =
          try Option(
                  KubernetesService.get(
                          client = kubernetesClient,
                          namespace = raphtoryKubernetesNamespaceName,
                          name = serviceName
                  )
          )
          catch {
            case e: Throwable =>
              raphtoryKubernetesLogger.error(
                      s"Error found when getting $serviceName service for $raphtoryComponent component",
                      e
              )
          }

        // TODO: This should check ingress and service in match, check with Ben on how this is done in scala
        service match {
          case None        =>
            raphtoryKubernetesLogger.debug(
                    s"Service $serviceName not found for $raphtoryComponent component. Ingress deployment aborted"
            )
          case Some(value) =>
            raphtoryKubernetesLogger.info(
                    s"Service $serviceName found for $raphtoryComponent component. Deploying ingress"
            )

            val ingressNamePrefix: String =
              s"raphtory-$raphtoryDeploymentId-$raphtoryComponent".toLowerCase()
            val ingressName: String       = s"$ingressNamePrefix-ingress"

            try KubernetesIngress.create(
                    client = kubernetesClient,
                    namespace = raphtoryKubernetesNamespaceName,
                    ingressConfig = KubernetesIngress.build(
                            name = ingressName,
                            annotations = Map(
                                    "nginx.ingress.kubernetes.io/rewrite-target" -> "/$2",
                                    "kubernetes.io/ingress.class"                -> "nginx"
                            ),
                            path = s"/$ingressNamePrefix(/|$$)(.*)",
                            backendServiceName = s"$ingressNamePrefix-svc",
                            backendServicePort = conf.getInt(
                                    s"raphtory.deploy.kubernetes.deployments.$raphtoryComponent.service.port"
                            )
                    )
            )
            catch {
              case e: Throwable =>
                raphtoryKubernetesLogger.error(
                        s"Error found when deploying ingress $ingressName for $raphtoryComponent component",
                        e
                )
            }
        }
      }
      else
        raphtoryKubernetesLogger.info(
                s"Setting raphtory.deploy.kubernetes.deployments.$raphtoryComponent.ingress.create is set to false"
        )
    }

  def delete(): Unit =
    raphtoryKubernetesDeployments.forEach { raphtoryComponent =>
      val ingressNamePrefix: String =
        s"raphtory-$raphtoryDeploymentId-$raphtoryComponent".toLowerCase()
      val ingressName: String       = s"$ingressNamePrefix-ingress"
      val ingress                   =
        try Option(
                KubernetesIngress.get(
                        client = kubernetesClient,
                        namespace = raphtoryKubernetesNamespaceName,
                        name = ingressName
                )
        )
        catch {
          case e: Throwable =>
            raphtoryKubernetesLogger.error(
                    s"Error found when getting ingress $ingressName for $raphtoryComponent component",
                    e
            )
        }

      ingress match {
        case None        =>
          raphtoryKubernetesLogger.debug(
                  s"Ingress $ingressName not found for $raphtoryComponent component"
          )
        case Some(value) =>
          raphtoryKubernetesLogger.info(
                  s"Ingress $ingressName found for $raphtoryComponent component. Deleting ingress"
          )
          try KubernetesIngress.delete(
                  client = kubernetesClient,
                  namespace = raphtoryKubernetesNamespaceName,
                  name = ingressName
          )
          catch {
            case e: Throwable =>
              raphtoryKubernetesLogger.error(
                      s"Error found when deleting ingress $ingressName for $raphtoryComponent component",
                      e
              )
          }
      }
    }
}
