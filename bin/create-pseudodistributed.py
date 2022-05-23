# Create folder structure for the different components
# Structure for Spout
# Structure for GraphBuilder
# Structure for PartitionManager
# Structure for Query Manager
# Copy the fatjar and the examples jar to each subfolder for the component


# Configure env variables for each component, we need possibly different ports for connection

# General env variables needed for everyone - same as in Kubernetes part


#! /home/rodrigo/yes/bin/python

import os, sys

os.environ['RAPHTORY_BUILD_SERVERS']="1"
os.environ['RAPHTORY_PARTITION_SERVERS']="1"
os.environ['RAPHTORY_BUILDERS_PER_SERVER']="1"
os.environ['RAPHTORY_PARTITIONS_PER_SERVER']="1"

os.environ['RAPHTORY_BIND_ADDRESS']="localhost"
os.environ['RAPHTORY_BIND_PORT']="1604"

os.environ['RAPHTORY_LEADER_ADDRESS']="localhost"
os.environ['RAPHTORY_LEADER_PORT']="1600"

# Change the RAM asigned to Java (Scala)
os.environ["JAVA_OPTS"]="-XX:+UseShenandoahGC -XX:+UseStringDeduplication -Xms2G -Xmx2G -Xss128M"

os.system('scala -classpath raphtory.jar:' + sys.argv[1] + " es.dit.upm.MultiRunner partitionManager")

os.