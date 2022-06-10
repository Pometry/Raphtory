# The script is a convenience wrapper for setting up the required variables for running Raphtory in pseudo distributed mode
# There needs to be pulsar already running. It is recommended to use the pulsar-local script for that purpose. Default urls are 
#  Preconfigured for it
# Usage: python bin/pseudodistributed/create/pseudodistributed <service name>.
# You need to run this script multiple times (we recommend to use separate shell tabs); either a batchpartitionmanager and a querymanager service, or a spout, builder, partitionmanager and querymanager set of services to run.
# The script copies the required jars (by default the examples lotr fatjar to a tmp folder unique to each service to allow them to run simultaneously.

#! /usr/bin/python3

import os, sys, shutil

service_name = sys.argv[1]

folder = "/tmp/raphtory/" + service_name


jar_folder = "examples/raphtory-example-lotr/target/scala-2.13/"
jar_name = "example-lotr-assembly-0.5.jar"
main_class = "com.raphtory.examples.lotrTopic.LOTRService"

try:
    os.mkdir(folder)
except OSError as error:
    print(error)    

shutil.copy(jar_folder + jar_name, folder)

os.environ["RAPHTORY_COMPONENTS_PARTITION_LOG"] = "DEBUG"
os.environ["RAPHTORY_COMPONENTS_QUERYMANAGER_LOG"] = "DEBUG"
os.environ["RAPHTORY_COMPONENTS_QUERYTRACKER_LOG"] = "DEBUG"
os.environ["RAPHTORY_COMPONENTS_SPOUT_LOG"] = "DEBUG"
os.environ["RAPHTORY_COMPONENTS_GRAPHBUILDER_LOG"] = "DEBUG"

os.environ['RAPHTORY_PULSAR_BROKER_ADDRESS']= "pulsar://127.0.0.1:6650"
os.environ['RAPHTORY_PULSAR_ADMIN_ADDRESS']= "http://127.0.0.1:8080"
os.environ['RAPHTORY_ZOOKEEPER_ADDRESS']= "127.0.0.1:2181"

os.chdir(folder)

# Change the RAM asigned to Java (Scala)
os.environ["JAVA_OPTS"]="-XX:+UseShenandoahGC -XX:+UseStringDeduplication -Xms1G -Xmx1G -Xss128M"

os.system('scala -classpath ' + jar_name + ' ' + main_class + ' ' + service_name)