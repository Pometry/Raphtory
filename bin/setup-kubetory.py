# Create folder structure for the different components
# Structure for Spout
# Structure for GraphBuilder
# Structure for PartitionManager
# Structure for Query Manager
# Copy the fatjar and the examples jar to each subfolder for the component


# Configure env variables for each component, we need possibly different ports for connection

# General env variables needed for everyone - same as in Kubernetes part


#! /home/rodrigo/yes/bin/python

import os, sys, shutil



jar_folder = "examples/raphtory-example-lotr/target/scala-2.13/"
jar_name = "example-lotr-0.5.jar"
main_class = "com.raphtory.deployment.kubernetes.Deploy"
core_jar = "core/target/scala-2.13/core-assembly-0.5.jar"
sa_file = "bin/sa.json"

docker_image = "docker.io/felixcdr/raphtory:fix"




os.environ['RAPHTORY_DEPLOY_ID']="kubetory"
os.environ['RAPHTORY_DEPLOY_KUBERNETES_MASTER_URL']="34.175.96.37"

os.environ['RAPHTORY_DEPLOY_KUBERNETES_SECRETS_REGISTRY_SERVER']="registry.docker.com"
os.environ['RAPHTORY_DEPLOY_KUBERNETES_SECRETS_REGISTRY_USERNAME']="felixcdr"
os.environ['RAPHTORY_DEPLOY_KUBERNETES_SECRETS_REGISTRY_PASSWORD']= "Wh1t3d0ck"
os.environ['RAPHTORY_DEPLOY_KUBERNETES_SECRETS_REGISTRY_EMAIL']="felix.cdr@gmail.com"


os.environ['RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_JAVA_RUN_CLASS']="com.raphtory.examples.lotrTopic.LOTRService"
os.environ['RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_PULSAR_BROKER_ADDRESS']="pulsar://pulsar-broker.pulsar.svc.cluster.local:6650"
os.environ['RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_PULSAR_ADMIN_ADDRESS']="http://pulsar-broker.pulsar.svc.cluster.local:8080"
os.environ['RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_ZOOKEEPER_ADDRESS']="pulsar-zookeeper.pulsar.svc.cluster.local:2181"
os.environ['RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_DEPLOY_ID']="kubetory"
os.environ['RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_PARTITIONS_SERVERCOUNT']='1'
os.environ['RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_ALL_PODS_ENV_RAPHTORY_PARTITIONS_COUNTPERSERVER']='1'
os.environ['RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_BUILDER_PODS_ENV_RAPHTORY_BUILDERS_COUNTPERSERVER']='1'
os.environ['RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_SPOUT_PODS_IMAGE']=docker_image
os.environ['RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_BUILDER_PODS_IMAGE']=docker_image
os.environ['RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_PARTITIONMANAGER_PODS_IMAGE']=docker_image
os.environ['RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_QUERYMANAGER_PODS_IMAGE']=docker_image
os.environ['RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_SPOUT_PODS_REPLICAS']='1'
os.environ['RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_BUILDER_PODS_REPLICAS']='1'
os.environ['RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_PARTITIONMANAGER_PODS_REPLICAS']='1'
os.environ['RAPHTORY_DEPLOY_KUBERNETES_DEPLOYMENTS_QUERYMANAGER_PODS_REPLICAS']='1'


os.environ["RAPHTORY_COMPONENTS_PARTITION_LOG"] = "DEBUG"
os.environ["RAPHTORY_COMPONENTS_QUERYMANAGER_LOG"] = "DEBUG"
os.environ["RAPHTORY_COMPONENTS_QUERYTRACKER_LOG"] = "DEBUG"
os.environ["RAPHTORY_COMPONENTS_SPOUT_LOG"] = "DEBUG"
os.environ["RAPHTORY_COMPONENTS_GRAPHBUILDER_LOG"] = "DEBUG"


# Change the RAM asigned to Java (Scala)
os.environ["JAVA_OPTS"]="-XX:+UseShenandoahGC -XX:+UseStringDeduplication -Xms1G -Xmx1G -Xss128M"

os.system('java -cp ' + jar_folder + '/' + jar_name + ':' + core_jar + ' ' + main_class)
