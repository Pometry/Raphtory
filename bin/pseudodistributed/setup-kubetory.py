# The script is a convenience wrapper for setting up the required variables for the Deploy scala script for Raphtory kubernetes
# There needs to be pulsar already running. It is recommended to use the pulsar-local script for that purpose. Default urls are 
#  Preconfigured for it
# Usage: python bin/pseudodistributed/create/pseudodistributed <service name>.
# You need to run this script multiple times (we recommend to use separate shell tabs); either a batchpartitionmanager and a querymanager service, or a spout, builder, partitionmanager and querymanager set of services to run.
# The script copies the required jars (by default the examples lotr fatjar to a tmp folder unique to each service to allow them to run simultaneously.

#! /usr/bin/python3

import os, sys, shutil

deploy_class = "com.raphtory.deployment.kubernetes.Deploy"

jar_folder = "examples/raphtory-example-lotr/target/scala-2.13/"
deploy_jar = "core/target/scala-2.13/core-assembly-0.1.0.jar"

#Full path of your pushed docker image (compatible with raphtory-docker-image script, and the main service class) 
docker_image = "docker.io/yourname/raphtory:yourtag"
service_class = "com.raphtory.examples.lotrTopic.LOTRService"

#name of your deployment id, can be changed
os.environ['RAPHTORY_DEPLOY_ID']="kubetory"

#url of the created kubernetes cluster where you want to reploy
os.environ['RAPHTORY_DEPLOY_KUBERNETES_MASTER_URL']="your.k8.cluster.url"

#credentials (must be user and password to connect to the docker registry where your image is published)
os.environ['RAPHTORY_DEPLOY_KUBERNETES_SECRETS_REGISTRY_SERVER']="registry.docker.com"
os.environ['RAPHTORY_DEPLOY_KUBERNETES_SECRETS_REGISTRY_USERNAME']="username"
os.environ['RAPHTORY_DEPLOY_KUBERNETES_SECRETS_REGISTRY_PASSWORD']= "password"
os.environ['RAPHTORY_DEPLOY_KUBERNETES_SECRETS_REGISTRY_EMAIL']="em@ail.com"

#the pulsar and zookeeper urls are based on having the pulsar helm chart deployed on the same k8 cluster raphtory will be deployed, on a namespace named pulsar
os.environ['RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_JAVA_RUN_CLASS']=service_class
os.environ['RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_PULSAR_BROKER_ADDRESS']="pulsar://pulsar-broker.pulsar.svc.cluster.local:6650"
os.environ['RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_PULSAR_ADMIN_ADDRESS']="http://pulsar-broker.pulsar.svc.cluster.local:8080"
os.environ['RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_ZOOKEEPER_ADDRESS']="pulsar-zookeeper.pulsar.svc.cluster.local:2181"
os.environ['RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_DEPLOY_ID']="kubetory"
os.environ['RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_PARTITIONS_SERVERCOUNT']='1'
os.environ['RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_PARTITIONS_COUNTPERSERVER']='1'
os.environ['RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_SPOUT_PODS_IMAGE']=docker_image
os.environ['RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_BUILDER_PODS_IMAGE']=docker_image
os.environ['RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_PARTITIONMANAGER_PODS_IMAGE']=docker_image
os.environ['RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_QUERYMANAGER_PODS_IMAGE']=docker_image
os.environ['RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_SPOUT_PODS_REPLICAS']='1'
os.environ['RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_BUILDER_PODS_REPLICAS']='1'
os.environ['RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_PARTITIONMANAGER_PODS_REPLICAS']='1'
os.environ['RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_QUERYMANAGER_PODS_REPLICAS']='1'

#debug levels, other env vars can be configured the same way
os.environ["RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_COMPONENTS_PARTITION_LOG"] = "DEBUG"
os.environ["RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_COMPONENTS_QUERYMANAGER_LOG"] = "DEBUG"
os.environ["RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_COMPONENTS_QUERYTRACKER_LOG"] = "DEBUG"
os.environ["RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_COMPONENTS_SPOUT_LOG"] = "DEBUG"
os.environ["RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_COMPONENTS_GRAPHBUILDER_LOG"] = "DEBUG"


# Change the RAM asigned to Java (Scala). Should not be needed for this script
os.environ["JAVA_OPTS"]="-XX:+UseShenandoahGC -XX:+UseStringDeduplication -Xms1G -Xmx1G -Xss128M"

os.system('java -cp ' + jar_folder + '/' +  core_jar + ' ' + main_class)