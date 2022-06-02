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


#os.environ['RAPHTORY_BUILD_SERVERS']="1"
#os.environ['RAPHTORY_PARTITION_SERVERS']="1"
#os.environ['RAPHTORY_BUILDERS_PER_SERVER']="1"
#os.environ['RAPHTORY_PARTITIONS_PER_SERVER']="1"

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
