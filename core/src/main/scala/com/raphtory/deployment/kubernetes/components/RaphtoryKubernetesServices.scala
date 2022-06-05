package com.raphtory.deployment.kubernetes.components

import com.raphtory.deployment.kubernetes.utils._

/** Extends KubernetesClient which extends Config.
  * KubernetesClient is used to establish kubernetes connection.
  * Kubernetes object config is read from application.conf values.
  *
  * @see [[com.raphtory.deployment.kubernetes.components.Config]]
  * [[com.raphtory.deployment.kubernetes.components.KubernetesClient]]
  */
object RaphtoryKubernetesServices extends KubernetesClient {

  /** Create kubernetes services needed for Raphtory (if toggled in application.conf) */
  def create(): Unit =
    raphtoryKubernetesDeployments.forEach { raphtoryComponent =>
      if (
              conf.hasPath(
                      s"raphtory.deploy.kubernetes.deployments.$raphtoryComponent.service.create"
              ) &&
              conf.getBoolean(
                      s"raphtory.deploy.kubernetes.deployments.$raphtoryComponent.service.create"
              )
      ) {
        val serviceName = s"raphtory-$raphtoryDeploymentId-$raphtoryComponent-svc".toLowerCase()

        raphtoryKubernetesLogger.info(
                s"Deploying raphtory $serviceName service for component $raphtoryComponent"
        )

        val labels: Map[String, String] = Map(
                "deployment"         -> "raphtory",
                "raphtory/job"       -> s"$raphtoryDeploymentId",
                "raphtory/component" -> s"$raphtoryComponent"
        )

        try {
          KubernetesService.create(
                  client = kubernetesClient,
                  namespace = raphtoryKubernetesNamespaceName,
                  serviceConfig = KubernetesService.build(
                          name = serviceName,
                          selectorLabels = labels,
                          labels = labels,
                          portName = conf.getString(
                                  s"raphtory.deploy.kubernetes.deployments.$raphtoryComponent.service.portName"
                          ),
                          portProtocol = conf.getString(
                                  s"raphtory.deploy.kubernetes.deployments.$raphtoryComponent.service.portProtocol"
                          ),
                          port = conf.getInt(
                                  s"raphtory.deploy.kubernetes.deployments.$raphtoryComponent.service.port"
                          ),
                          targetPort = conf.getInt(
                                  s"raphtory.deploy.kubernetes.deployments.$raphtoryComponent.service.targetPort"
                          ),
                          serviceType = conf.getString(
                                  s"raphtory.deploy.kubernetes.deployments.$raphtoryComponent.service.type"
                          )
                  )
          )

          conf.getBoolean(
                  s"raphtory.deploy.kubernetes.deployments.$raphtoryComponent.service.portName"
          )

        }
        catch {
          case e: Throwable =>
            raphtoryKubernetesLogger.error(
                    s"Error found when deploying $serviceName service for $raphtoryComponent component",
                    e
            )
        }
      }
      else
        raphtoryKubernetesLogger.info(
                s"Setting raphtory.deploy.kubernetes.deployments.$raphtoryComponent.service.create is set to false"
        )
    }

  /** Delete kubernetes services needed for Raphtory (if toggled in application.conf) */
  def delete(): Unit =
    raphtoryKubernetesDeployments.forEach { raphtoryComponent =>
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

      service match {
        case None        =>
          raphtoryKubernetesLogger.debug(
                  s"Service $serviceName not found for $raphtoryComponent. Service delete aborted"
          )
        case Some(value) =>
          raphtoryKubernetesLogger.info(
                  s"Service $serviceName found for $raphtoryComponent component. Deleting service"
          )
          try KubernetesService.delete(
                  client = kubernetesClient,
                  namespace = raphtoryKubernetesNamespaceName,
                  name = serviceName
          )
          catch {
            case e: Throwable =>
              raphtoryKubernetesLogger.error(
                      s"Error found when deleting $serviceName service for $raphtoryComponent component",
                      e
              )
          }
      }
    }
}
