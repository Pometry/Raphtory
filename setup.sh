#!/usr/bin/env bash

#get the local IP address which must be passed to akka
IP="$(./getMyIP.sh)"

#kill off all old containers
./dockblock.sh

#PORTS for all containers managed within this script
SeedPort=9101
RestPort=9102
Router1Port=9103
Router2Port=9104
PM0Port=9105
PM1Port=9106

#Number of partitions must be known before hand as there is currently no addition of partitions once running
NumberOfPartitions=2

#Specify the location of the docker image
Image="dockerexp/cluster" #local if you build your own from the source files
#Image=blah #if you want to use prebuilt one on my quay.io

#check if a log file exists, if it does not create it
if [ ! -d logs ]; then mkdir logs; fi
#if [ ! -d logs/testEntityLogs/ ]; then mkdir logs/testEntityLogs/; fi //only needed for serialized testing
if [ ! -d logs/entityLogs/ ]; then mkdir logs/entityLogs/; fi

#get full log file paths to pass to partition mangers for logging
entityLogs=$(pwd)"/logs/entityLogs"

#Runs seed node, check read me for seed node explanation
(docker run -p $SeedPort:2551 --rm -e "HOST_IP=$IP" -e "HOST_PORT=$SeedPort" $Image seed &) > logs/seed.txt
echo "Seed node up and running at $IP:$SeedPort"

#Runs Rest API Node
(docker run -p $RestPort:2551 -p 8080:8080 --rm -e "HOST_IP=$IP" -e "HOST_PORT=$RestPort" $Image rest $IP:$SeedPort &) > logs/rest.txt
echo "REST API node up and running at $IP:$RestPort"

#Run first router node
(docker run -p $Router1Port:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$Router1Port" $Image router $IP:$SeedPort $NumberOfPartitions &) > logs/router1.txt
echo "Router 1 up and running at $IP:$Router1Port"

#Run second router node
(docker run -p $Router2Port:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$Router2Port" $Image router $IP:$SeedPort $NumberOfPartitions &) > logs/router2.txt
echo "Router 2 up and running at $IP:$Router2Port"

#Run first Partition manager
PM0ID=0
(docker run -p $PM0Port:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$PM0Port" -v $entityLogs:/logs/entityLogs $Image partitionManager $IP:$SeedPort $PM0ID $NumberOfPartitions &) > logs/partitionManager0.txt
echo "Partition Manager $PM0ID up and running at $IP:$PM0Port"

#Run second Partition manager
PM1ID=1
(docker run -p $PM1Port:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$PM1ID" -v $entityLogs:/logs/entityLogs $Image partitionManager $IP:$SeedPort $PM1ID $NumberOfPartitions &) > logs/partitionManager1.txt
echo "Partition Manager $PM1ID up and running at $IP:$PM1Port"