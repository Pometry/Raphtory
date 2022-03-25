# Kubernetes Cluster Deployment

## Prerequisites

* Raphtory compiled fat jar
* Jar file containing any extra classes
* A running kubernetes cluster
* Sufficient access to the kubernetes cluster from local machine
* Java 11 installed on local machine
* A running pulsar cluster that is accessible from the running Kubernetes cluster
* Nginx controller

## How the deployment works

The current kubernetes implementation is a set of deployments in Kubernetes. For each component, a deployment is added into Kubernetes and the deployment and pod spec is set from the variables that are passed into the scala Deploy command.

The deployment will create the following objects in Kubernetes:
* Kubernetes namespace
* Kubernetes secret containing registry details
* Kubernetes deployment for each component which in turn will spawn the right number of pods (dictated by replicas setting)
* Kubernetes service attached to each component's pods (if toggle set to true)
* Kubernetes ingress attached to each component's service (if toggle set to true)

## Creating deployable artifact

To run in kubernetes as a deployment, users must supply images that can be deployed into kubernetes pod containers. Currently the images that are built to run in kubernetes are built in a Docker container runtime environment.

In the Raphtory codebase there is a Dockerfile supplied under `bin/kubernetes/docker/raphtory` dir that can be used to run Raphtory in kubernetes. This file can be used as is or can be adjusted to match your individual needs.

When deploying the docker image into Kubernetes, it is expected that the docker image contains the jar files that are needed to run Raphtory and any custom classes.

Raphtory codebase also contains the `raphtory-docker-image` script under the `bin` directory which when run will copy the jars that are provided into the docker container and then package into a deployable docker image. The script also has an option to push the image to your docker registry that will be used by Kubernetes to pull images.

When running the push command, you must be logged into your docker repository. You can login using this command `docker login example.url.com --username “xxxxxx” --password "xxxxxxx"`

Example usage of the script
```
./bin/raphtory-docker-image -c build -t tag -r example.url.com/docker/repo -z "/path/to/raphtory/jar/raphtory-fat-jar-0.1.jar /path/to/classes/jar.jar"
./bin/raphtory-docker-image -c push -t tag -r example.url.com/docker/repo -z "/path/to/raphtory/jar/raphtory-fat-jar-0.1.jar /path/to/classes/jar.jar"
```

The outcome wil be a docker image that can run Raphtory including the required jars on the classpath, stored in your repo and ready for use.

## Configuration Parameters

When running the `com.raphtory.deployment.kubernetes.Deploy` class to deploy, you can pass in parameters using java arguments at runtime or by setting environment variables pre run.

To deploy Raphtory, you must set the following parameters

| Java                                                                  | Environment Variable                                                  | Type    | Description                                                                        |
|-----------------------------------------------------------------------|-----------------------------------------------------------------------|---------|------------------------------------------------------------------------------------|
| raphtory.deploy.id                                                    | RAPHTORY_DEPLOY_ID                                                    | String  | Deploy ID for this deployment (string)                                             |
| raphtory.deploy.kubernetes.master.url                                 | RAPHTORY_DEPLOY_KUBERNETES_MASTER_URL                                 | String  | Master URL for connection to Kubernetes cluster                                    |
| raphtory.deploy.kubernetes.secrets.registry.server                    | RAPHTORY_DEPLOY_KUBERNETES_SECRETS_REGISTRY_SERVER                    | String  | Docker image registry server                                                       |
| raphtory.deploy.kubernetes.secrets.registry.username                  | RAPHTORY_DEPLOY_KUBERNETES_SECRETS_REGISTRY_USERNAME                  | String  | Docker image registry username                                                     |
| raphtory.deploy.kubernetes.secrets.registry.password                  | RAPHTORY_DEPLOY_KUBERNETES_SECRETS_REGISTRY_PASSWORD                  | String  | Docker image registry password                                                     |
| raphtory.deploy.kubernetes.secrets.registry.email                     | RAPHTORY_DEPLOY_KUBERNETES_SECRETS_REGISTRY_EMAIL                     | String  | Docker image registry email                                                        |
| raphtory.deploy.kubernetes.deployments.spout.pods.replicas            | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_SPOUT_PODS_REPLICAS            | String  | Replica count for spout pods, will be added into spout deployment spec             |
| raphtory.deploy.kubernetes.deployments.builder.pods.replicas          | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_BUILDER_PODS_REPLICAS          | String  | Replica count for builder pods, will be added into spout deployment spec           |
| raphtory.deploy.kubernetes.deployments.partitionmanager.pods.replicas | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_PARTITIONMANAGER_PODS_REPLICAS | String  | Replica count for partition manager pods, will be added into spout deployment spec |
| raphtory.deploy.kubernetes.deployments.querymanager.pods.replicas     | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_QUERYMANAGER_PODS_REPLICAS     | String  | Replica count for query manager pods, will be added into spout deployment spec     |
| raphtory.deploy.kubernetes.deployments.spout.pods.image               | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_SPOUT_PODS_IMAGE               | String  | Image to use for spout pods, will be added into spout pod spec                     |
| raphtory.deploy.kubernetes.deployments.builder.pods.image             | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_BUILDER_PODS_IMAGE             | String  | Image to use for builder pods, will be added into spout pod spec                   |
| raphtory.deploy.kubernetes.deployments.partitionmanager.pods.image    | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_PARTITIONMANAGER_PODS_IMAGE    | String  | Image to use for partition manager pods, will be added into spout pod spec         |
| raphtory.deploy.kubernetes.deployments.querymanager.pods.image        | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_QUERYMANAGER_PODS_IMAGE        | String  | Image to use for query manager pods, will be added into spout pod spec             |

To configure the kubernetes deployable components, you can also set the following

| Java                                                                         | Environment Variable                                                          | Type    | Description                                                |
|------------------------------------------------------------------------------|-------------------------------------------------------------------------------|---------|------------------------------------------------------------|
| raphtory.deploy.kubernetes.namespace.create                                  | RAPHTORY_DEPLOY_KUBERNETES_NAMESPACE_CREATE                                   | Boolean | Toggle for creation of namespace                           |
| raphtory.deploy.kubernetes.namespace.name                                    | RAPHTORY_DEPLOY_KUBERNETES_NAMESPACE_NAME                                     | String  | Name of the namespace that will be deployed into           |
| raphtory.deploy.kubernetes.deployments.spout.create                          | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_SPOUT_CREATE                           | Boolean | Toggle for creation of spout deployment                    |
| raphtory.deploy.kubernetes.deployments.spout.pods.port                       | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_SPOUT_PODS_PORT                        | Int     | ContainerPort to be used in spout deployment               |
| raphtory.deploy.kubernetes.deployments.spout.service.create                  | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_SPOUT_SERVICE_CREATE                   | Boolean | Toggle for creation of spout service                       |
| raphtory.deploy.kubernetes.deployments.spout.service.portName                | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_SPOUT_SERVICE_PORT_NAME                | String  | Port name to be used on spout service                      |
| raphtory.deploy.kubernetes.deployments.spout.service.portProtocol            | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_SPOUT_SERVICE_PORT_PROTOCOL            | String  | Port protocol to be used on spout service                  |
| raphtory.deploy.kubernetes.deployments.spout.service.port                    | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_SPOUT_SERVICE_PORT                     | Int     | Port number to be used on spout service                    |
| raphtory.deploy.kubernetes.deployments.spout.service.targetPort              | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_SPOUT_SERVICE_TARGET_PORT              | Int     | Target Port number to be used on spout service             |
| raphtory.deploy.kubernetes.deployments.spout.service.type                    | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_SPOUT_SERVICE_TYPE                     | String  | Type to be used on spout service                           |
| raphtory.deploy.kubernetes.deployments.spout.ingress                         | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_SPOUT_INGRESS_CREATE                   | Boolean | Toggle for creation of spout ingress                       |
| raphtory.deploy.kubernetes.deployments.builder.create                        | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_BUILDER_CREATE                         | Boolean | Toggle for creation of builder deployment                  |
| raphtory.deploy.kubernetes.deployments.builder.pods.port                     | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_BUILDER_PODS_PORT                      | Int     | ContainerPort to be used in builder deployment             |
| raphtory.deploy.kubernetes.deployments.builder.service.create                | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_BUILDER_SERVICE_CREATE                 | Boolean | Toggle for creation of builder service                     |
| raphtory.deploy.kubernetes.deployments.builder.service.portName              | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_BUILDER_SERVICE_PORT_NAME              | String  | Port name to be used on builder service                    |
| raphtory.deploy.kubernetes.deployments.builder.service.portProtocol          | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_BUILDER_SERVICE_PORT_PROTOCOL          | String  | Port protocol to be used on builder service                |
| raphtory.deploy.kubernetes.deployments.builder.service.port                  | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_BUILDER_SERVICE_PORT                   | Int     | Port number to be used on builder service                  |
| raphtory.deploy.kubernetes.deployments.builder.service.targetPort            | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_BUILDER_SERVICE_TARGET_PORT            | Int     | Target Port number to be used on builder service           |
| raphtory.deploy.kubernetes.deployments.builder.service.type                  | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_BUILDER_SERVICE_TYPE                   | String  | Type to be used on builder service                         |
| raphtory.deploy.kubernetes.deployments.builder.ingress                       | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_BUILDER_INGRESS_CREATE                 | Boolean | Toggle for creation of builder ingress                     |
| raphtory.deploy.kubernetes.deployments.partitionmanager.create               | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_PARTITIONMANAGER_CREATE                | Boolean | Toggle for creation of partition manager deployment        |
| raphtory.deploy.kubernetes.deployments.partitionmanager.pods.port            | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_PARTITIONMANAGER_PODS_PORT             | Int     | ContainerPort to be used in partition manager deployment   |
| raphtory.deploy.kubernetes.deployments.partitionmanager.service.create       | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_PARTITIONMANAGER_SERVICE_CREATE        | Boolean | Toggle for creation of partition manager service           |
| raphtory.deploy.kubernetes.deployments.partitionmanager.service.portName     | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_PARTITIONMANAGER_SERVICE_PORT_NAME     | String  | Port name to be used on partition manager service          |
| raphtory.deploy.kubernetes.deployments.partitionmanager.service.portProtocol | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_PARTITIONMANAGER_SERVICE_PORT_PROTOCOL | String  | Port protocol to be used on partition manager service      |
| raphtory.deploy.kubernetes.deployments.partitionmanager.service.port         | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_PARTITIONMANAGER_SERVICE_PORT          | Int     | Port number to be used on partition manager service        |
| raphtory.deploy.kubernetes.deployments.partitionmanager.service.targetPort   | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_PARTITIONMANAGER_SERVICE_TARGET_PORT   | Int     | Target Port number to be used on partition manager service |
| raphtory.deploy.kubernetes.deployments.partitionmanager.service.type         | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_PARTITIONMANAGER_SERVICE_TYPE          | String  | Type to be used on partition manager service               |
| raphtory.deploy.kubernetes.deployments.partitionmanager.ingress              | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_PARTITIONMANAGER_INGRESS_CREATE        | Boolean | Toggle for creation of partition manager ingress           |
| raphtory.deploy.kubernetes.deployments.querymanager.create                   | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_QUERYMANAGER_CREATE                    | Boolean | Toggle for creation of query manager deployment            |
| raphtory.deploy.kubernetes.deployments.querymanager.pods.port                | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_QUERYMANAGER_PODS_PORT                 | Int     | ContainerPort to be used in query manager deployment       |
| raphtory.deploy.kubernetes.deployments.querymanager.service.create           | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_QUERYMANAGER_SERVICE_CREATE            | Boolean | Toggle for creation of query manager service               |
| raphtory.deploy.kubernetes.deployments.querymanager.service.portName         | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_QUERYMANAGER_SERVICE_PORT_NAME         | String  | Port name to be used on query manager service              |
| raphtory.deploy.kubernetes.deployments.querymanager.service.portProtocol     | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_QUERYMANAGER_SERVICE_PORT_PROTOCOL     | String  | Port protocol to be used on query manager service          |
| raphtory.deploy.kubernetes.deployments.querymanager.service.port             | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_QUERYMANAGER_SERVICE_PORT              | Int     | Port number to be used on query manager service            |
| raphtory.deploy.kubernetes.deployments.querymanager.service.targetPort       | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_QUERYMANAGER_SERVICE_TARGET_PORT       | Int     | Target Port number to be used on query manager service     |
| raphtory.deploy.kubernetes.deployments.querymanager.service.type             | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_QUERYMANAGER_SERVICE_TYPE              | String  | Type to be used on query manager service                   |
| raphtory.deploy.kubernetes.deployments.querymanager.ingress                  | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_QUERYMANAGER_INGRESS_CREATE            | Boolean | Toggle for creation of query manager ingress               |


To configure the deployment run settings, raphtory will read in exported environment variables with certain prefixes.

`RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_` prefix is used to define env vars that will be added to all pods
`RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_<raphtory_component>_PODS_ENV_` prefix where `<raphtory_component>` is replaced with the component name is used to define env vars that will be added to the relevant component.

When the deployment is adding these environmnent variables to the kubernetes pods, the prefixes are removed.

The text that follows the prefix in the key is used as the key for the environment variable that is added to the container.

Example of how to add run time environment variables to all pods or to specific component pods:

```
export RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_ENVIRONMENT_VARABLE="environment variable added to all component pods"
export RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_SPOUT_PODS_ENV_ENVIRONMENT_VARABLE="environment variable added to spout component pods"
export RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_PARTITIONMANAGER_PODS_ENV_ENVIRONMENT_VARABLE="environment variable added to partition manager component pods"
export RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_QUERYMANAGER_PODS_ENV_ENVIRONMENT_VARABLE="environment variable added to query manager component pods"
export RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_BUILDER_PODS_ENV_ENVIRONMENT_VARABLE="environment variable added to builder component pods"
```

Required run time settings

| Java                                                                                     | Environment Variable                                                                     | Type    | Description |
|------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------|---------|-------------|
| raphtory.deploy.kubernetes.deployments.all.pods.env.RAPHTORY_JAVA_RUN_CLASS              | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_JAVA_RUN_CLASS              | String  | Class that you wish to run when running the raphtory components |
| raphtory.deploy.kubernetes.deployments.all.pods.env.RAPHTORY_PULSAR_BROKER_ADDRESS       | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_PULSAR_BROKER_ADDRESS       | String  | Pulsar broker address pulsar://<url>:<port> |
| raphtory.deploy.kubernetes.deployments.all.pods.env.RAPHTORY_PULSAR_ADMIN_ADDRESS        | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_PULSAR_ADMIN_ADDRESS        | String  | Pulsar admin address http://<url>:<port>    |
| raphtory.deploy.kubernetes.deployments.all.pods.env.RAPHTORY_ZOOKEEPER_ADDRESS           | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_ZOOKEEPER_ADDRESS           | String  | Zookeeper address <url>:<port>              |
| raphtory.deploy.kubernetes.deployments.all.pods.env.RAPHTORY_DEPLOY_ID                   | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_DEPLOY_ID                   | String  | Deploy ID for internal raphtory labels      |
| raphtory.deploy.kubernetes.deployments.all.pods.env.RAPHTORY_PARTITIONS_SERVERCOUNT      | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_PARTITIONS_SERVERCOUNT      | Int     | Count of partition manager servers          |
| raphtory.deploy.kubernetes.deployments.all.pods.env.RAPHTORY_PARTITIONS_COUNTPERSERVER   | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_PARTITIONS_COUNTPERSERVER   | Int     | Count of partition managers on each server  |
| raphtory.deploy.kubernetes.deployments.builder.pods.env.RAPHTORY_BUILDERS_COUNTPERSERVER | RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_BUILDER_PODS_ENV_RAPHTORY_BUILDERS_COUNTPERSERVER | Int     | Count of builder servers                    |

Example of required settings being set using environment variables:

```
export RAPHTORY_DEPLOY_ID="id"
export RAPHTORY_DEPLOY_KUBERNETES_MASTER_URL="https://kubernetes.master.url"
export RAPHTORY_DEPLOY_KUBERNETES_SECRETS_REGISTRY_SERVER=registry.docker.com
export RAPHTORY_DEPLOY_KUBERNETES_SECRETS_REGISTRY_USERNAME=xxxxxxxxx
export RAPHTORY_DEPLOY_KUBERNETES_SECRETS_REGISTRY_PASSWORD=xxxxxxxxxx
export RAPHTORY_DEPLOY_KUBERNETES_SECRETS_REGISTRY_EMAIL=user@email.com
export RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_JAVA_RUN_CLASS="com.raphtory.develop.EthereumDistributedTest"
export RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_PULSAR_BROKER_ADDRESS="pulsar://pulsar.url:6650"
export RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_PULSAR_ADMIN_ADDRESS="http://pulsar.url:8080"
export RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_ZOOKEEPER_ADDRESS="pulsar.url:2181"
export RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_DEPLOY_ID="id"
export RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_PARTITIONS_SERVERCOUNT=1
export RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_PARTITIONS_COUNTPERSERVER=1
export RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_BUILDER_PODS_ENV_RAPHTORY_BUILDERS_COUNTPERSERVER=1
export RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_SPOUT_PODS_IMAGE="registry.docker.com/raphtory:tag"
export RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_BUILDER_PODS_IMAGE="registry.docker.com/raphtory:tag"
export RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_PARTITIONMANAGER_PODS_IMAGE="registry.docker.com/raphtory:tag"
export RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_QUERYMANAGER_PODS_IMAGE="registry.docker.com/raphtory:tag"
export RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_SPOUT_PODS_REPLICAS=1
export RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_BUILDER_PODS_REPLICAS=2
export RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_PARTITIONMANAGER_PODS_REPLICAS=2
export RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_QUERYMANAGER_PODS_REPLICAS=1
```

Example of other configuration options being set using environment variables. You can see the configurable settings in the application.conf

```
export RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_DATA_CONTAINSDELETIONS=false
export RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_SPOUT_PODS_ENV_RAPHTORY_SPOUT_SOURCEDIRECTORY=/raphtory/data
export RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_SPOUT_PODS_ENV_RAPHTORY_SPOUT_FILEFILTER="^.*\\.([gG][zZ]??)$"
export RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_SPOUT_PODS_ENV_RAPHTORY_SPOUT_RECURSE=false
export RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_PARTITIONMANAGER_PODS_ENV_RAPHTORY_PARTITIONS_BATCHMESSAGES=true
```

## Deploy Raphtory to Kubernetes

To deploy Raphtory into Kubernetes, run the `com.raphtory.deployment.kubernetes.Deploy` class using a fat jar on the classpath of the java command.

Configuring the deployment can be done using environment variables or by passing config arguments to the java command.

Example command if environment variables are already set

```
java -cp /path/to/raphtory/jar/raphtory-fat-jar-0.1.jar com.raphtory.deployment.kubernetes.Deploy
```

Example command if setting config arguments when running java

```
java -cp /path/to/raphtory/jar/raphtory-fat-jar-0.1.jar -D<java_configuration_key>="value" com.raphtory.deployment.kubernetes.Deploy
```

## Remove Raphtory from Kubernetes
```
java -cp /path/to/raphtory/jar/raphtory-fat-jar-0.1.jar com.raphtory.deployment.kubernetes.Delete
```
