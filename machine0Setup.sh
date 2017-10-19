#!/usr/bin/env bash 
 
IP="$(./getMyIP.sh)" 
 
ZooKeeper="138.37.32.88:2181,138.37.32.86:2181" 
 
Image="quay.io/miratepuffin/cluster" #if you want to use prebuilt one on my quay.io 
 
NumberOfPartitions=5
 
JVM="-Dcom.sun.management.jmxremote.rmi.port=9090 -Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.port=9090  -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.local.only=false -Djava.rmi.server.hostname=$IP" 
if [ ! -d logs ]; then mkdir logs; fi 
rm -r logs/machine0Setup
if [ ! -d logs/machine0Setup ]; then mkdir logs/machine0Setup; fi 
if [ ! -d logs/machine0Setup/entityLogs/ ]; then mkdir logs/machine0Setup/entityLogs/; fi 
entityLogs=$(pwd)"/logs/machine0Setup/entityLogs" 
 
chmod 777 logs 
chmod 777 logs/machine0Setup
chmod 777 logs/machine0Setup/entityLogs
 
SeedPort=9101 
RestPort=9102 
UpdatePort=9103 
BenchmarkPort=9104 
 
LiveAnalysisPort=9105 
 
ClusterUpPort=9106 
 
PM0Port=9200
PM2Port=9202
PM4Port=9204
PM0ID=0
PM2ID=2
PM4ID=4
(docker run -p $PM0Port:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$PM0Port" -v $entityLogs:/logs/entityLogs $Image partitionManager $PM0ID $NumberOfPartitions $ZooKeeper &) > logs/machine0Setup/partitionManager0.txt 
sleep 2 
echo "Partition Manager $PM0ID up and running at $IP:$PM0Port" 
 
(docker run -p $PM2Port:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$PM2Port" -v $entityLogs:/logs/entityLogs $Image partitionManager $PM2ID $NumberOfPartitions $ZooKeeper &) > logs/machine0Setup/partitionManager2.txt 
sleep 2 
echo "Partition Manager $PM2ID up and running at $IP:$PM2Port" 
 
(docker run -p $PM4Port:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$PM4Port" -v $entityLogs:/logs/entityLogs $Image partitionManager $PM4ID $NumberOfPartitions $ZooKeeper &) > logs/machine0Setup/partitionManager4.txt 
sleep 2 
echo "Partition Manager $PM4ID up and running at $IP:$PM4Port" 
 
Router0Port=9300
Router2Port=9302
Router4Port=9304
(docker run -p $Router0Port:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$Router0Port" $Image router $NumberOfPartitions $ZooKeeper &) > logs/machine0Setup/router0.txt 
sleep 1 
echo "Router 0 up and running at $IP:$Router0Port" 
 
(docker run -p $Router2Port:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$Router2Port" $Image router $NumberOfPartitions $ZooKeeper &) > logs/machine0Setup/router2.txt 
sleep 1 
echo "Router 2 up and running at $IP:$Router2Port" 
 
(docker run -p $Router4Port:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$Router4Port" $Image router $NumberOfPartitions $ZooKeeper &) > logs/machine0Setup/router4.txt 
sleep 1 
echo "Router 4 up and running at $IP:$Router4Port" 
 
