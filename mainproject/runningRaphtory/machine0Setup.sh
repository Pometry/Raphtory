#!/usr/bin/env bash 
 
IP="$(./getMyIP.sh)" 
 
docker ps -aq --no-trunc | xargs docker rm
 
ZooKeeper="138.37.32.88:2181" 
 
LAMName="testLam" 
 
Image="quay.io/miratepuffin/cluster" #if you want to use prebuilt one on my quay.io 
 
NumberOfPartitions=1
 
NumberOfUpdates=0
 
JVM="-Dcom.sun.management.jmxremote.rmi.port=9090 -Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.port=9090  -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.local.only=false -Djava.rmi.server.hostname=$IP" 
if [ ! -d logs ]; then mkdir logs; fi 
rm -r logs/machine0Setup
if [ ! -d logs/machine0Setup ]; then mkdir logs/machine0Setup; fi 
if [ ! -d logs/machine0Setup/entityLogs/ ]; then mkdir logs/machine0Setup/entityLogs/; fi 
entityLogs=$(pwd)"/logs/machine0Setup/entityLogs" 
 
chmod 777 logs 
chmod 777 logs/machine0Setup
chmod 777 logs/machine0Setup/entityLogs
 
PM0Port=9200
PM0ID=0
(docker run --name="partition0"  -p $PM0Port:$PM0Port  --rm -e "BIND_PORT=$PM0Port" -e "HOST_IP=$IP" -e "HOST_PORT=$PM0Port" -v $entityLogs:/logs/entityLogs $Image partitionManager $PM0ID $NumberOfPartitions $ZooKeeper &) > logs/machine0Setup/partitionManager0.txt 
sleep 2 
echo "Partition Manager $PM0ID up and running at $IP:$PM0Port" 
 
Router0Port=9300
(docker run --name="router0" -p $Router0Port:$Router0Port  --rm -e "BIND_PORT=$Router0Port" -e "HOST_IP=$IP" -e "HOST_PORT=$Router0Port" $Image router $NumberOfPartitions $ZooKeeper &) > logs/machine0Setup/router0.txt 
sleep 1 
echo "Router 0 up and running at $IP:$Router0Port" 
 
Update0Port=9400
(docker run --name="updater0" -p $Update0Port:$Update0Port  --rm -e "BIND_PORT=$Update0Port" -e "HOST_IP=$IP" -e "HOST_PORT=$Update0Port" $Image updateGen $NumberOfPartitions $NumberOfUpdates $ZooKeeper &) > logs/machine0Setup/updateGenerator0.txt 
sleep 1 
echo "Update Generator 0 up and running at $IP:$Update0Port" 
 
ClusterUpPort=9106 
 
(docker run --name="cluster up"  -p $ClusterUpPort:$ClusterUpPort  --rm -e "BIND_PORT=$ClusterUpPort" -e "HOST_IP=$IP" -e "HOST_PORT=$ClusterUpPort" $Image ClusterUp $NumberOfPartitions $ZooKeeper &) > logs/machine0Setup/ClusterUp.txt 
sleep 15 
echo "CLUSTER UP"
 
