package components

import utils._

/** Extends KubernetesClient which extends Config.
  * KubernetesClient is used to establish kubernetes connection.
  * Kubernetes objects that are iterated over are read from application.conf values.
  *
  * @see [[Config]]
  * [[KubernetesClient]]
  * [[KubernetesNamespace]]
  */
object RaphtoryKubernetesNamespaces extends KubernetesClient {

  /** Create kubernetes ingresses needed for Raphtory (if toggled in application.conf) */
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

  /** Delete kubernetes ingresses needed for Raphtory (if toggled in application.conf) */
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
